"""Real-Time Statistical Anomaly Detector for Financial Market Streams.

This module provides a sliding-window Z-Score detector equipped with a
Dual-Condition Guard designed for high-frequency financial time-series.

Key Mathematical Concepts:
1. Rolling Baseline:
   Maintains a per-ticker circular buffer (collections.deque) of historical trade
   prices to dynamically compute the rolling mean (μ) and standard deviation (σ).
2. Z-Score Profiling:
   Calculates how many standard deviations an incoming price sits away from the
   established baseline:
       Z = (P_t - μ) / σ
3. Dual-Condition Guard (Anti Micro-Volatility Trap):
   In extremely quiet markets (σ ≈ 0), a tiny cent fluctuation can produce an
   artificially massive Z-Score. The detector guards against false positives by
   requiring both:
       (a) Statistical Outlier: |Z| > z_threshold (e.g. 3.0, representing <0.27% probability)
       (b) Material Shock: |P_t - μ| / μ >= min_pct_change (e.g. 2.0% price move)
"""

from collections import defaultdict, deque
import numpy as np


class RollingZScoreDetector:
    """Detects price anomalies across streaming assets using sliding-window statistics.

    Parameters:
        window_size: Maximum capacity of the rolling circular buffer per asset.
        z_threshold: Number of standard deviations required to flag an anomaly (default 3.0).
        min_observations: Minimum number of historical ticks required before scoring (cold-start guard).
        min_pct_change: Minimum relative price move (|P_t - μ| / μ) required to trigger an alert.
    """

    def __init__(
        self,
        window_size: int = 100,
        z_threshold: float = 3.0,
        min_observations: int = 20,
        min_pct_change: float = 0.02,
    ):
        self.window_size = window_size
        self.z_threshold = z_threshold
        self.min_observations = min_observations
        self.min_pct_change = min_pct_change

        # Circular buffer per ticker: ticker -> deque(maxlen=window_size)
        # O(1) appends and automatic eviction of oldest ticks with zero array reallocations.
        self.price_history: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=window_size))

    def evaluate(self, ticker: str, price: float) -> tuple[bool, float]:
        """Evaluates an incoming trade price against the asset's rolling baseline.

        The incoming price is evaluated against the historical baseline BEFORE being
        appended to the window to prevent extreme outliers from distorting their
        own Z-Score (avoiding the statistical 'masking effect').

        Args:
            ticker: The financial asset symbol (e.g., 'PETR4', 'VALE3').
            price: The latest executed trade price.

        Returns:
            tuple[bool, float]:
                - is_anomaly (bool): True if the tick triggers the dual-condition guard.
                - z_score (float): The calculated Z-score (standard deviations from mean).
        """
        history = self.price_history[ticker]

        # ----------------------------------------------------------------------
        # 1. Cold-Start Guard
        # ----------------------------------------------------------------------
        # Standard deviation and mean are statistically unreliable with too few samples.
        # Warm up the circular buffer before scoring anomalies.
        if len(history) < self.min_observations:
            history.append(price)
            return False, 0.0

        # ----------------------------------------------------------------------
        # 2. Compute Baseline Statistics
        # ----------------------------------------------------------------------
        # Compute baseline gravity center (mean μ) and volatility (std dev σ)
        mean = float(np.mean(history))
        std_deviation = float(np.std(history))

        # ----------------------------------------------------------------------
        # 3. Guard Against Zero-Volatility (Micro-Volatility Trap)
        # ----------------------------------------------------------------------
        # Floor effective volatility at 1 cent ($0.01) to prevent division-by-zero errors.
        std_effective = max(std_deviation, 0.01)
        z_score = (price - mean) / std_effective

        # Calculate material percentage change relative to baseline
        pct_change = abs(price - mean) / mean if mean > 0.0 else 0.0

        # ----------------------------------------------------------------------
        # 4. Dual-Condition Anomaly Verification
        # ----------------------------------------------------------------------
        # Must be BOTH a statistical outlier (|Z| > threshold) AND a financially
        # material shock (percentage move >= min_pct_change).
        is_anomaly = (abs(z_score) > self.z_threshold) and (pct_change >= self.min_pct_change)

        # ----------------------------------------------------------------------
        # 5. Window Ingestion (Regime Adaptation)
        # ----------------------------------------------------------------------
        # Append the price to allow the window to adapt to genuine regime shifts
        history.append(price)

        return bool(is_anomaly), float(z_score)