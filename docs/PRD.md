# PRD — Cashflow Lakehouse

## 1. Overview
Cashflow Lakehouse is a PySpark + Delta Lake batch pipeline for financial
transaction data. It ingests raw transactions, cleans and upserts them with
ACID guarantees, aggregates business metrics, benchmarks performance
optimizations, and trains an ML fraud-detection model — all on Spark,
validated locally on a sample and scaled to a distributed Azure Databricks
cluster on ~20-30GB of synthetically-expanded data.

## 2. Motive
Existing portfolio projects (SWMS, dataflow-etl) cover streaming ingestion
(Kafka+Spark) and dbt-based ELT on Postgres. Neither demonstrates: ACID
lakehouse architecture at scale, measurable Spark performance optimization,
or ML built directly on a Spark-native feature layer. This project closes
that gap and reinforces the Databricks Fundamentals certification with a
real Azure Databricks deployment.

## 3. Problem Statement
Organizations running batch analytics on raw data lakes (plain Parquet/CSV)
face three recurring pains:
- **No ACID guarantees** — concurrent writes corrupt data or produce
  inconsistent reads.
- **Poor query performance at scale** — naive partitioning and join
  strategies don't hold up as data volume grows.
- **Fragmented ML pipelines** — feature engineering is disconnected from the
  batch ETL layer, causing stale, untraceable features.

## 4. Proposed Solution
A unified Spark-based lakehouse pipeline using Delta Lake for ACID batch
processing across a Bronze → Silver → Gold medallion architecture, with:
- Built-in performance optimization (broadcast joins, AQE, Z-ordering),
  benchmarked against a naive baseline with real numbers.
- An ML feature and scoring layer built directly on the Gold layer, so
  features stay fresh and traceable back to raw data.

## 5. Scope

### In scope
- Synthetic finance transaction data (PaySim-seeded, scaled via PySpark to
  20-30GB)
- Bronze/Silver/Gold Delta Lake pipeline
- Delta `MERGE` upserts and schema evolution demo
- Performance benchmark module (naive vs optimized config)
- MLlib fraud-detection model + batch scoring
- Local dev on a data sample; final run on Azure Databricks + ADLS Gen2
- Databricks Workflows orchestration (Bronze → Silver → Gold → Benchmark → ML)
- Public GitHub repo with README, architecture diagrams, benchmark results

### Out of scope
- Real-time/streaming ingestion (already covered by SWMS)
- Production-grade access control / multi-tenant security
- CI/CD beyond basic GitHub Actions (lint/test), no full deployment pipeline
- Real (non-synthetic) financial data

## 6. Success Criteria
- Pipeline runs end-to-end locally on a sample and on Azure Databricks on
  the full scaled dataset
- Benchmark section shows a measurable, real performance improvement
  (naive vs optimized config)
- ML model produces a reasonable fraud-detection metric (e.g. AUC/F1) on
  held-out data
- Repo is portfolio-ready: clean README, architecture diagrams, results,
  and a written retrospective

## 7. POC (first milestone)
- ~500MB sample
- Bronze → Silver with one Delta MERGE upsert
- One benchmark: shuffle join vs broadcast join, with real numbers
- Proves the architecture end-to-end before scaling up
