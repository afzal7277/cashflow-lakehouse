# LLD — Cashflow Lakehouse

## 1. Data Generator (`generator/generate_transactions.py`)

### Schema (PaySim-based)
| Column | Type | Notes |
|---|---|---|
| step | int | time unit (1 step = 1 hour) |
| type | string | CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER |
| amount | double | skewed distribution, higher variance for TRANSFER/CASH_OUT |
| nameOrig | string | origin account ID (Faker-generated) |
| oldbalanceOrg | double | origin balance before txn |
| newbalanceOrig | double | origin balance after txn |
| nameDest | string | destination account ID |
| oldbalanceDest | double | destination balance before txn |
| newbalanceDest | double | destination balance after txn |
| isFraud | int | label, ~0.1-1% of rows |
| isFlaggedFraud | int | rule-based flag (e.g. large TRANSFER) |

### Config
- `NUM_ROWS`: env-controlled, default 5,000,000 locally
- `FRAUD_RATIO`: default 0.005
- Output: partitioned Parquet or directly to Delta at `data/raw/transactions`

## 2. Bronze (`bronze/ingest.py`)
- Read generator output (or raw source file)
- Add `ingest_timestamp`, `source_file` columns
- Write as Delta table, append mode, no dedup yet
- Path: `data/bronze/transactions`

## 3. Silver (`silver/transform.py`)
- Read Bronze Delta table
- Drop nulls in key columns (`nameOrig`, `amount`, `type`)
- Cast types explicitly (avoid implicit inference drift)
- Deduplicate on a synthetic transaction ID (hash of orig+dest+step+amount)
- `MERGE INTO` silver table on transaction ID (upsert demo)
- Schema evolution test: add `channel` column in a later run, verify no break
- Path: `data/silver/transactions`

## 4. Gold (`gold/aggregate.py`)
- Read Silver Delta table
- Aggregations:
  - Daily transaction volume & total amount by `type`
  - Fraud rate by `type` and time bucket
  - Top origin/destination accounts by volume
- Partition by date bucket; Z-order by `type`
- Path: `data/gold/daily_metrics`, `data/gold/fraud_summary`

## 5. Benchmark Module (`benchmarks/run_benchmark.py`)
- Two runs of the same Silver→Gold join logic:
  - **Naive**: default shuffle partitions (200), no broadcast hint, AQE off
  - **Optimized**: tuned shuffle partitions, broadcast hint on small dim
    table, AQE on, Z-ordered Gold table
- Capture: wall-clock time, shuffle read/write size (from Spark UI /
  `spark.sparkContext.statusTracker()`), stage count
- Output: `benchmarks/results.csv` + chart (matplotlib) → embedded in README

## 6. ML Layer (`ml/features.py`, `ml/train.py`, `ml/score.py`)
- **Features** (from Gold + Silver): txn amount, type (one-hot), balance
  delta, hour-of-day, rolling txn count per account (window function)
- **Model**: MLlib `RandomForestClassifier` or `GBTClassifier`, target =
  `isFraud`
- **Split**: time-based train/test (avoid leakage)
- **Metrics**: AUC-ROC, precision/recall, F1 — logged to `ml/metrics.json`
- **Batch scoring**: `ml/score.py` applies trained model to new Silver data,
  writes predictions to `data/gold/fraud_predictions` (Delta)

## 7. Orchestration (Databricks Workflows)
DAG: `generate` → `bronze_ingest` → `silver_transform` → `gold_aggregate` →
(`benchmark` in parallel) → `ml_train` → `ml_score`

## 8. Testing (`tests/`)
- `test_generator.py` — schema/row count/fraud ratio sanity checks
- `test_silver_merge.py` — verify MERGE upsert correctness on a small fixture
- `test_gold_aggregates.py` — verify aggregation totals against known fixture
- Run via `pytest` locally before every cluster run

## 9. Local vs Cluster Config Differences
| Setting | Local | Databricks |
|---|---|---|
| master | `local[*]` | cluster-managed |
| shuffle.partitions | 8 | 200+ (tuned per cluster size) |
| driver.memory | 4g | cluster default |
| storage | local `data/` dir | ADLS Gen2 path (`abfss://...`) |
