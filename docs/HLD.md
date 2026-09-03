# HLD — Cashflow Lakehouse

## 1. Target Platform
Azure Databricks + ADLS Gen2 (Delta Lake) for the final run.
Local machine + Databricks Community Edition for development.

## 2. Architecture Overview

```
┌─────────────┐     ┌──────────────────────────────────────────┐     ┌─────────────┐
│  Raw Data    │────▶│           Azure Databricks                │────▶│  Consumers  │
│  Generator   │     │                                            │     │             │
│ (PaySim-     │     │  ┌────────┐   ┌────────┐   ┌────────┐    │     │  README /   │
│  seeded,     │     │  │ BRONZE │──▶│ SILVER │──▶│  GOLD  │    │     │  benchmarks │
│  scaled)     │     │  │ (raw)  │   │(clean) │   │ (agg)  │    │     │  ML metrics │
└─────────────┘     │  └────────┘   └────────┘   └────────┘    │     └─────────────┘
                     │       │            │            │         │
                     │       ▼            ▼            ▼         │
                     │            ADLS Gen2 (Delta Lake)          │
                     └──────────────────────────────────────────┘
                                    │
                          ┌─────────┴──────────┐
                          │  MLlib Feature Eng   │
                          │  + Model Training     │
                          │  + Batch Scoring      │
                          └───────────────────────┘
```

## 3. Components

### 3.1 Data Generator
- PySpark job, PaySim-style schema
- `NUM_ROWS` parameter — small locally, large on cluster
- Injects controllable fraud ratio and skew

### 3.2 Bronze (raw ingestion)
- Batch read of generated/raw data → Delta table
- Schema-on-read, minimal transform
- Captures ingestion metadata (load timestamp, source file)

### 3.3 Silver (cleaned)
- Null handling, dedup, type casting
- `MERGE INTO` for upserts (ACID/CDC proof)
- Schema evolution demo (add a column mid-project without breaking)

### 3.4 Gold (aggregated)
- Business metrics: daily transaction volume, fraud rate by type/geo, etc.
- Partitioned + Z-ordered for query performance

### 3.5 Benchmark Module
- Runs identical Silver/Gold job twice: naive config vs optimized
  (broadcast join, AQE on, Z-order)
- Captures execution time / query plan → benchmarks table + chart

### 3.6 ML Layer
- Feature engineering from Gold layer
- MLlib model (fraud classification)
- Batch scoring job writing predictions to a Delta table

### 3.7 Orchestration
- Databricks Workflows (Jobs), DAG: Generator → Bronze → Silver → Gold →
  Benchmark → ML

## 4. Non-Functional Goals
- ACID guarantees on lake storage (Delta)
- Reproducible, benchmarked performance improvements (measured, not claimed)
- ML features traceable back to raw data (lineage)
- Same codebase runs unchanged from local sample to cluster-scale data

## 5. Deployment Phases
1. **Local dev** — small sample (1-2GB), `local[*]` Spark, iterate on logic
2. **Cluster validation** — Databricks Community Edition, same code
3. **Production-scale run** — Azure Databricks cluster + ADLS Gen2,
   20-30GB scaled dataset, auto-terminating cluster to control cost
