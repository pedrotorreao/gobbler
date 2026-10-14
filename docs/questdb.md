# QuestDB: Architecture, InfluxDB Line Protocol & Financial Analytics Guide

This document summarizes the architectural concepts, storage engine mechanics, wire protocols, and time-series querying patterns for **QuestDB** as utilized in the Gobbler Market Surveillance platform.

---

## 1. What is QuestDB?

**QuestDB** is an open-source, high-performance time-series database engineered specifically for real-time analytics, financial market data, and high-frequency streaming workloads.

* **Hybrid C++ and Java Core**: Engineered from the ground up to eliminate Garbage Collection (GC) pauses on the write path, employing zero-copy append semantics and SIMD-vectorized query execution.
* **Dual Ingestion & Querying Interfaces**:
  * **Ingestion**: Native support for the **InfluxDB Line Protocol (ILP)** over TCP and HTTP, ingesting millions of rows per second directly into memory-mapped columnar files without SQL parsing overhead.
  * **Querying**: Full ANSI SQL engine with specialized financial and time-series extensions (`SAMPLE BY`, `ASOF JOIN`, `LATEST ON`), queryable via PostgreSQL Wire Protocol (`:8812`) or the embedded Web Console (`:9000`).
* **Role in Gobbler**: Serves as the persistent time-series storage layer in Phase 4. It stores partitioned raw ticks (`market_ticks`) and ML/statistical alerts (`market_anomalies`), enabling microsecond surveillance queries and historical market auditing.

---

## 2. Storage Engine Mechanics: Columnar vs. Row Stores

To understand why QuestDB can sustain 50,000+ msgs/sec while maintaining sub-millisecond query latencies, we must contrast its storage engine with traditional relational databases.

```text
Row-Oriented Storage (PostgreSQL, MySQL):
Disk Block 1: [Tick 1: Time, Ticker, Price, Volume] [Tick 2: Time, Ticker, Price, Volume]
Disk Block 2: [Tick 3: Time, Ticker, Price, Volume] ...
-> Inefficient for aggregates: Must load ALL columns into CPU cache to scan 'price'.

Columnar Storage (QuestDB):
timestamp.d: [Time 1]   [Time 2]   [Time 3]   [Time 4] ...
ticker.d:    [SymKey 1] [SymKey 2] [SymKey 3] [SymKey 4] ...
price.d:     [38.45]    [38.50]    [38.48]    [38.52]  ... (Contiguous doubles)
volume.d:    [1200]     [800]      [1500]     [950]    ... (Contiguous longs)
-> SIMD Vectorization: CPU loads contiguous arrays into AVX-512 registers in parallel!
```

### 1. Zero-Copy Append & Direct Column Files
Each table column is stored as an independent, append-only file on disk (`.d`). When an incoming tick is appended:
* The database writes directly to memory-mapped buffers (`mmap`) backing each column.
* There is no row serialization, page fragmentation, or write-ahead-log (WAL) replay penalty on the primary ILP ingestion path.

### 2. Symbol Interning (Dictionary Encoding)
Low-cardinality string columns (such as asset tickers: `PETR4`, `VALE3`) are declared as the **`SYMBOL`** data type:
* Instead of storing 5 to 10 bytes of ASCII string per row, QuestDB assigns each unique ticker an integer key (`int32`).
* A separate symbol table maps string names to keys. The column data file stores only the 4-byte integer.
* **Benefits**: Reduces disk and RAM footprints by up to 80% and converts expensive string filtering (`ticker = 'PETR4'`) into instantaneous $O(1)$ CPU integer comparisons.

---

## 3. Ingestion Protocol: InfluxDB Line Protocol (ILP)

Gobbler uses QuestDB's **InfluxDB Line Protocol** rather than SQL `INSERT` statements over PostgreSQL wire.

### Why ILP over PostgreSQL Wire (PGWire)?

| Feature | PostgreSQL Wire (`INSERT INTO ...`) | InfluxDB Line Protocol (ILP) |
| :--- | :--- | :--- |
| **Parsing Overhead** | Heavy: Tokenizes SQL string, builds AST, validates schema. | Minimal: Lightweight delimiter scanning (` `, `,`, `=`). |
| **Engine Execution** | Passes through relational query planner & transaction manager. | Bypasses SQL engine; streams directly into column memory buffers. |
| **Throughput Ceiling** | ~10,000 – 30,000 rows/sec per node. | **1,000,000+ rows/sec** per node. |
| **Schema Flexibility** | Strict: Fails if columns or table do not exist. | **Schema-on-Write**: Automatically creates missing tables and columns. |

### ILP Wire Syntax Breakdown

An ILP record consists of four distinct sections delimited by spaces and commas:

```text
<table_name>[,<tag_key>=<tag_value>...] <field_key>=<field_value>... [<timestamp_nanos>]
```

```text
market_ticks,ticker=PETR4 price=38.45,volume=1200i 1728187200000000000\n
│            └──────────┘ └───────────────────────┘ └─────────────────┘
│                 │                   │                      │
▼                 ▼                   ▼                      ▼
Table Name   Tag / Symbol        Field Set             Designated Timestamp
             (Indexed, Interned) (Numeric Primitives)  (UTC Nanoseconds)
```

#### Syntax Rules & Data Types in QuestDB:
1. **Tags (Symbols)**: Follow the table name after a comma. Stored as interned `SYMBOL` columns.
   * Format: `ticker=PETR4`
2. **Fields**: Separated from tags by a space. Stored as raw column values.
   * **Floating-point**: `price=38.45` (default numeric without suffix -> `DOUBLE`).
   * **Integer**: `volume=1200i` (**Must** have trailing `i` suffix, otherwise QuestDB interprets it as float).
   * **Boolean**: `is_anomaly=t` or `is_anomaly=true`.
   * **String**: `tier="STATISTICAL"` (quoted strings -> `STRING` or `VARCHAR`).
3. **Designated Timestamp**: Trailing integer representing nanoseconds since the Unix epoch (`1970-01-01T00:00:00Z`).
4. **Delimiter**: Every line **must** terminate with a newline character (`\n`).

### Transport: TCP Socket vs. HTTP Endpoint
QuestDB supports ILP over two transports:
* **TCP Socket (`localhost:9009`)**: Raw TCP connection. Extremely low latency, zero HTTP framing overhead. Client writes a continuous byte stream of newline-delimited ILP.
* **HTTP (`http://localhost:9000/write`)**: Standard HTTP POST with chunked transfer encoding. Provides explicit HTTP response status codes (`204 No Content` on success, `400 Bad Request` with exact column syntax error details), enabling straightforward error handling and backpressure detection.

---

## 4. Table Schema & Partitioning Strategy

In time-series surveillance, partition architecture dictates query efficiency, file handle usage, and disk I/O performance.

### Gobbler Two-Table Architecture

Rather than forcing raw ticks and anomaly alerts into a single table with sparse columns, Gobbler maintains two specialized tables:

```text
┌──────────────────────────────────────┐       ┌──────────────────────────────────────┐
│             market_ticks             │       │           market_anomalies           │
├──────────────────────────────────────┤       ├──────────────────────────────────────┤
│ ticker       SYMBOL (CAPACITY 16)    │       │ ticker       SYMBOL (CAPACITY 16)    │
│ price        DOUBLE                  │       │ price        DOUBLE                  │
│ volume       LONG                    │       │ volume       LONG                    │
│ timestamp    TIMESTAMP (Designated)  │       │ z_score      DOUBLE                  │
├──────────────────────────────────────┤       │ ml_score     DOUBLE                  │
│ PARTITION BY DAY                     │       │ anomaly_tier SYMBOL (CAPACITY 8)     │
│ ~50,000 rows/sec                     │       │ timestamp    TIMESTAMP (Designated)  │
└──────────────────────────────────────┘       ├──────────────────────────────────────┤
                                               │ PARTITION BY DAY                     │
                                               │ ~10-50 rows/sec                      │
                                               └──────────────────────────────────────┘
```

### Table DDL (Pre-Creation)

While ILP can create tables automatically on write, explicitly declaring tables allows defining optimal symbol caching and designated timestamp parameters:

```sql
-- 1. High-Volume Raw Market Ticks
CREATE TABLE IF NOT EXISTS market_ticks (
    ticker SYMBOL CAPACITY 16 CACHE,
    price DOUBLE,
    volume LONG,
    timestamp TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY DAY;

-- 2. Surveillance Anomaly Audit Log
CREATE TABLE IF NOT EXISTS market_anomalies (
    ticker SYMBOL CAPACITY 16 CACHE,
    price DOUBLE,
    volume LONG,
    z_score DOUBLE,
    ml_score DOUBLE,
    anomaly_tier SYMBOL CAPACITY 8 CACHE,
    timestamp TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY DAY;
```

#### Why `CAPACITY 16 CACHE`?
* `CAPACITY`: Anticipated distinct symbol count. For our 10 B3 assets (`PETR4`, `VALE3`, etc.), pre-allocating capacity 16 eliminates symbol table rehashing.
* `CACHE`: Keeps the string-to-key symbol mapping memory-resident for $O(1)$ lookups during ingestion.

#### Partitioning Rationale (`PARTITION BY DAY`)
* **Physical Directory Layout**: QuestDB stores partitions as isolated directory slices on disk (e.g., `db/market_ticks/2026-10-06/`).
* **Sizing Rule of Thumb**: Partitions should ideally range from **100 MB to 10 GB**.
* **Daily vs. Hourly**:
  * For local development, tests, and burst runs, `PARTITION BY DAY` prevents directory proliferation and bounds open file descriptors.
  * In a 24/7 sustained 50,000 tick/sec production environment (~180 million rows/hour, ~2 GB/hour), `PARTITION BY HOUR` can be chosen to maintain granular disk segments.

---

## 5. Financial SQL Analytics & Native Extensions

QuestDB implements SQL extensions tailored specifically for financial quantitative analysis and market surveillance:

### 1. `SAMPLE BY` (Real-Time Resampling & Candlesticks)
Aggregates ticks into fixed time buckets (1 second, 5 seconds, 1 minute) to compute open-high-low-close-volume (OHLCV) bars on the fly:

```sql
SELECT 
    timestamp,
    ticker,
    first(price) AS open,
    max(price)   AS high,
    min(price)   AS low,
    last(price)  AS close,
    sum(volume)  AS volume
FROM market_ticks
WHERE ticker = 'PETR4' AND timestamp > dateadd('h', -1, now())
SAMPLE BY 1s ALIGN TO CALENDAR;
```

### 2. `LATEST ON` (Instantaneous Order Book Snapshot)
Finds the most recent state for each individual asset without requiring expensive correlated subqueries:

```sql
SELECT timestamp, ticker, price, volume
FROM market_ticks
LATEST ON timestamp PARTITION BY ticker;
```

### 3. `ASOF JOIN` (Temporal Point-in-Time Joins)
In financial surveillance, an anomaly alert generated at $T_a$ needs to be evaluated against the prevailing market tick at or immediately before that instant. Standard relational joins fail because timestamps rarely match down to the exact nanosecond.

`ASOF JOIN` matches each row from the left table with the closest preceding timestamp record from the right table:

```sql
SELECT 
    a.timestamp AS alert_time,
    a.ticker,
    a.anomaly_tier,
    a.z_score,
    a.ml_score,
    t.price AS prevailing_price,
    t.volume AS prevailing_volume
FROM market_anomalies a
ASOF JOIN market_ticks t ON (ticker);
```

---

## 6. Docker Compose Service Definition

In [`docker-compose.yml`](file:///home/pedrotorreao/Documents/Projects/gobbler/docker-compose.yml):

```yaml
  questdb:
    image: questdb/questdb:latest
    container_name: questdb
    restart: unless-stopped
    ports:
      - "9000:9000" # Web Console, REST API, and HTTP ILP (/write)
      - "9009:9009" # InfluxDB Line Protocol (ILP) over TCP
      - "8812:8812" # PostgreSQL Wire Protocol
    environment:
      - QDB_TELEMETRY_ENABLED=false
    # Ephemeral testing mode: no volume mount declared (cleared on restart)
```

---

## 7. Developer Cheat Sheet: Interacting with QuestDB

### 1. Access the Web Console
Open your browser to:
```text
http://localhost:9000
```
The built-in web studio provides interactive SQL execution, table schema inspection, and real-time chart visualization.

### 2. Manual HTTP ILP Test (`curl`)
Send a synthetic tick directly via the `/write` endpoint:

```bash
curl -i -X POST "http://localhost:9000/write" \
  --data-binary "market_ticks,ticker=PETR4 price=38.45,volume=1000i $(date +%s%N)"
```

### 3. Query via REST API
Execute SQL queries programmatically via HTTP GET:

```bash
curl -G "http://localhost:9000/exec" \
  --data-urlencode "query=SELECT count(*) FROM market_ticks;"
```

### 4. Connect via PostgreSQL CLI (`psql`)
Connect to QuestDB using any standard PostgreSQL client:

```bash
psql -h localhost -p 8812 -U admin -d qdb
```
*(Default credentials: user `admin`, password `quest`, database `qdb`)*
