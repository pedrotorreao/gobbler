"""Real-Time Statistical Anomaly Detector for Financial Market Streams.

This module provides a two-tier, sliding-window anomaly detector for high-frequency
financial time-series. Each tier is an independent detector targeting a complementary
class of market anomalies:

Tier 1 — Statistical Profiling (Dual-Condition Z-Score Guard):
    Maintains a per-ticker circular buffer (collections.deque) of `(price, volume)`
    tuples to dynamically compute rolling statistics. An anomaly is flagged only when
    BOTH conditions hold simultaneously:
        (a) Statistical Outlier: |Z| > z_threshold  (e.g. 3.0, i.e. <0.27% probability)
            Z = (P_t - μ) / σ_effective
        (b) Material Shock: |P_t - μ| / μ >= min_pct_change  (e.g. 2.0% price move)
    The AND-gate prevents the Micro-Volatility Trap: in frozen or illiquid markets
    (σ ≈ 0), even a $0.01 price wiggle produces a massive Z-score. Requiring both
    a statistical outlier AND a minimum percentage move eliminates these false positives.

Tier 2 — Unsupervised ML (IsolationForest on Normalized Feature Space):
    An sklearn.ensemble.IsolationForest is fitted on the rolling window every
    `refit_interval` ticks. Features are normalized to rolling Z-scores before
    training and inference:
        features = [z_price, z_volume] = [(P_t - μ_P) / σ_P, (V_t - μ_V) / σ_V]
    Normalization serves two purposes:
        1. Scale Equalization: Raw price (cents range) and volume (hundreds range)
           have incompatible scales. Without normalization, volume dominates all
           random coordinate splits in the isolation trees.
        2. Stationarity: Normalizing to Z-scores always centers the normal cluster
           at (0, 0) regardless of market price level or regime drift.
    Anomaly scoring uses score_samples() (continuous path-length score) rather than
    predict() (binary contamination threshold) to avoid the Contamination Trap:
    with contamination=0.02, predict() guarantees 2% of any training window is
    labeled -1, even when no real anomalies exist.

Detection Logic (Independent OR):
    is_anomaly = tier1_anomaly  OR  (ml_scored_sample < -0.65)
    Either tier can independently trigger an alert:
    - Tier 1 catches: sharp univariate price shocks (flash crashes, pumps).
    - Tier 2 catches: multivariate co-movement anomalies (e.g., extreme volume
      spikes with no significant price change, which Tier 1 cannot detect).

Zero-Volatility Guard:
    A single effective std array floors both axes before any division:
        stds_effective = np.maximum(stds, [0.01, 1])
    - Price floor: $0.01 (minimum equity tick size).
    - Volume floor: 1 share (minimum atomic volume unit).
    This prevents ZeroDivisionError and NaN corruption of the training matrix when
    a stock is halted or all window volumes are identical.
"""

from collections import defaultdict, deque
import numpy as np
from sklearn.ensemble import IsolationForest
from enum import IntEnum

class AnomalyTierIndicator(IntEnum):
    """Identifies which detection tier(s) triggered a confirmed anomaly.

    Returned as the fourth element of RollingZScoreDetector.evaluate().
    Serialized as its string name (e.g., 'STATISTICAL') in the anomaly
    JSON payload emitted to the 'market-anomalies' topic.

    Members:
        NONE (0):        No anomaly detected. Default value when is_anomaly is False.
        STATISTICAL (1): Only Tier 1 (Z-Score Dual-Condition Guard) triggered.
                         Indicates a sharp univariate price shock (flash crash, pump).
        ML (2):          Only Tier 2 (IsolationForest score_samples) triggered.
                         Indicates a multivariate co-movement anomaly (e.g., volume
                         spike with no significant price move — invisible to Tier 1).
        BOTH (3):        Both tiers independently triggered on the same tick.
                         Highest confidence anomaly signal.
    """
    NONE = 0
    STATISTICAL = 1
    ML = 2
    BOTH = 3

class RollingZScoreDetector:
    """Detects price and volume anomalies across streaming assets using a two-tier approach.

    Tier 1 (Statistical): A Dual-Condition Z-Score guard flags univariate price shocks.
    Tier 2 (ML): An IsolationForest trained on normalized [z_price, z_volume] features
    flags multivariate co-movement anomalies (e.g., pure volume spikes) that Tier 1
    cannot detect. Both tiers are independent — either can trigger an alert.

    Parameters:
        window_size: Maximum capacity of the rolling circular buffer per asset (default 100).
        z_threshold: Number of standard deviations required for a Tier 1 outlier (default 3.0).
        min_observations: Minimum buffer size before scoring begins; guards against cold-start
            instability where std and mean are statistically unreliable (default 20).
        min_pct_change: Minimum relative price move required alongside the Z-score outlier
            to confirm a Tier 1 anomaly. Prevents false positives in frozen markets
            where σ ≈ 0 inflates Z-scores on noise (default 0.02 = 2%).
        refit_interval: Number of ticks between periodic IsolationForest re-fits.
            Keeps the ML model aligned with the current market regime (default 100).
        ml_score_delay: Number of ticks to wait before computing the IsolationForest
            score. This allows the ML model to build a more robust baseline before
            scoring (default 500).
    """

    def __init__(
        self,
        window_size: int = 100,
        z_threshold: float = 3.0,
        min_observations: int = 20,
        min_pct_change: float = 0.02,
        refit_interval: int = 100,
        ml_score_delay: int = 500
    ):
        self.window_size = window_size
        self.z_threshold = z_threshold
        self.min_observations = min_observations
        self.min_pct_change = min_pct_change
        self.refit_interval = refit_interval
        self.ml_score_delay = ml_score_delay

        # Circular buffer per ticker: ticker -> deque(maxlen=window_size)
        # O(1) appends and automatic eviction of oldest ticks with zero array reallocations.
        # Ticker -> deque of (price, volume) tuples
        self.asset_history: dict[str, deque[tuple[float, int]]] = defaultdict(lambda: deque(maxlen=window_size))
        # Ticker -> IsolationForest instance
        self.models: dict[str, IsolationForest] = defaultdict(lambda: IsolationForest(
            n_estimators=50,        # 50 trees cuts CPU inference latency in half (< 0.5ms)
            contamination=0.02,     # Calibrates the anomaly boundary for a 2% baseline outlier rate
            random_state=42,        # Deterministic and reproducible splits
        ))
        # Ticker -> tick counter for periodic re-fitting
        self.tick_counters = defaultdict(int)

    def evaluate(self, ticker: str, price: float, volume: int) -> tuple[bool, float, float, AnomalyTierIndicator]:
        """Evaluates an incoming trade tick against the asset's rolling two-tier baseline.

        Evaluation Order:
            1. Cold-Start Guard: Returns early if the buffer is under-populated.
            2. Baseline Statistics: Computes rolling mean and floored std from the window.
            3. Tier 1: Checks the Dual-Condition Z-Score guard on price.
            4. Tier 2: Once tick_counters[ticker] >= ml_score_delay, fits (or re-fits)
               the IsolationForest on normalized window data and scores the incoming
               [z_price, z_volume] feature vector. Gated by ml_score_delay to ensure
               the model trains on a representative regime before its scores are trusted.
            5. Anomaly Decision: is_anomaly = tier1_anomaly OR (ml_score < -0.65).
            6. Window Ingestion: Appends the tick AFTER scoring to prevent the masking
               effect — extreme outliers should not distort their own Z-score.

        Args:
            ticker: The financial asset symbol (e.g., 'PETR4', 'VALE3').
            price: The latest executed trade price in currency units.
            volume: The latest executed trade volume in number of shares/contracts.
                Used as the second feature dimension for the IsolationForest. A volume
                spike alone (without a price shock) can trigger a Tier 2 alert even
                when Tier 1 remains silent.

        Returns:
            tuple[bool, float, float, AnomalyTierIndicator]:
                - is_anomaly (bool): True if either Tier 1 or Tier 2 detects an anomaly.
                - z_score (float): The Tier 1 price Z-score (standard deviations from rolling
                    mean). Returns 0.0 during the cold-start warm-up period.
                - ml_score (float): The Tier 2 IsolationForest score_samples output.
                    Interpretation: > -0.45 normal cluster, -0.45 to -0.60 borderline,
                    < -0.65 isolated outlier. Returns 0.0 during the ml_score_delay
                    warm-up period (first ml_score_delay ticks per ticker).
                - anomaly_tier (AnomalyTierIndicator): Identifies which tier(s) fired.
                    AnomalyTierIndicator.NONE when is_anomaly is False.
                    AnomalyTierIndicator.STATISTICAL when only Tier 1 triggered.
                    AnomalyTierIndicator.ML when only Tier 2 triggered.
                    AnomalyTierIndicator.BOTH when both tiers fired on the same tick
                    (highest confidence signal).
        """
        history = self.asset_history[ticker]

        # ----------------------------------------------------------------------
        # 1. Cold-Start Guard
        # ----------------------------------------------------------------------
        # Standard deviation and mean are statistically unreliable with too few samples.
        # Warm up the circular buffer before scoring anomalies.
        if len(history) < self.min_observations:
            history.append((price, int(volume)))
            self.tick_counters[ticker] += 1
            return False, 0.0, 0.0, AnomalyTierIndicator.NONE

        # ----------------------------------------------------------------------
        # 2. Compute Baseline Statistics
        # ----------------------------------------------------------------------
        # Compute baseline gravity center (mean μ) and volatility (std dev σ)
        data = np.array(history)
        means = np.mean(data, axis=0)
        stds = np.std(data, axis=0)

        # ----------------------------------------------------------------------
        # 3. Guard Against Zero-Volatility (Micro-Volatility Trap)
        # ----------------------------------------------------------------------
        # Floor effective volatility at 1 cent ($0.01) to prevent division-by-zero errors.
        stds_effective = np.maximum(stds, [0.01, 1])
        z_price = (price - means[0]) / stds_effective[0]
        pct_change = abs(price - means[0]) / means[0] if means[0] > 0.0 else 0.0
        
        # Tier 1: Dual-Condition Z-Score on Price
        tier1_anomaly = (abs(z_price) > self.z_threshold) and (pct_change >= self.min_pct_change)

        # ----------------------------------------------------------------------
        # 4. Calculate Isolation Forest Anomaly Score (Tier 2 — Unsupervised ML)
        # ----------------------------------------------------------------------
        ml_scored_sample = 0.0

        if self.tick_counters[ticker] >= self.ml_score_delay:
            # 1. Fit the model as soon as the warm-up period (ml_score_delay=500) concludes,
            #    and periodically re-fit every refit_interval (100) ticks to adapt to regime drift.
            #    LEARNING NOTE — scikit-learn Fitted Attribute Convention ('estimators_'):
            #    In scikit-learn, attributes ending with a trailing underscore (such as
            #    'estimators_') are created ONLY after .fit() has been called at least once.
            #    Checking 'not hasattr(self.models[ticker], "estimators_")' is the idiomatic
            #    O(1) way to verify whether this ticker's IsolationForest is still unfitted.
            history_normalized = (data - means) / stds_effective
            if (not hasattr(self.models[ticker], "estimators_")) or (self.tick_counters[ticker] % self.refit_interval == 0):
                self.models[ticker].fit(np.array(history_normalized))
        
            # 2. Score sample in normalized 2D [z_price, z_volume] space:
            #    score_samples() returns the opposite of the anomaly score defined in the
            #    original Isolation Forest paper (Liu et al., 2008):
            #      > -0.45        : Dense normal cluster (long average tree path length)
            #      -0.45 to -0.65 : Borderline transition zone
            #      < -0.65        : Geometrically isolated outlier (short average path length)
            price_normalized = (price - means[0]) / stds_effective[0]
            volume_normalized = (volume - means[1]) / stds_effective[1]
            ml_scored_sample = self.models[ticker].score_samples([[price_normalized, volume_normalized]])[0]
        
        # ----------------------------------------------------------------------
        # 5. Two-Tier Anomaly Decision (Independent OR)
        # ----------------------------------------------------------------------
        # Either tier independently triggers an alert:
        #   - Tier 1 (tier1_anomaly): Price Z-score exceeds threshold AND percentage
        #     move is financially material. Catches sharp univariate price shocks.
        #   - Tier 2 (ml_scored_sample < -0.65): IsolationForest path-length score
        #     indicates the [z_price, z_volume] feature vector is geometrically isolated
        #     from the normal cluster. Catches multivariate co-movement anomalies,
        #     including pure volume spikes that produce no significant price movement.
        is_anomaly = tier1_anomaly or (ml_scored_sample < -0.65)

        anomaly_tier = AnomalyTierIndicator.NONE
        if is_anomaly:
            if tier1_anomaly and ml_scored_sample < -0.65:
                anomaly_tier = AnomalyTierIndicator.BOTH
            elif tier1_anomaly:
                anomaly_tier = AnomalyTierIndicator.STATISTICAL
            else:
                anomaly_tier = AnomalyTierIndicator.ML

        # ----------------------------------------------------------------------
        # 6. Window Ingestion (Regime Adaptation)
        # ----------------------------------------------------------------------
        # Append the price to allow the window to adapt to genuine regime shifts
        history.append((price, volume))

        self.tick_counters[ticker] += 1

        return is_anomaly, float(z_price), float(ml_scored_sample), anomaly_tier