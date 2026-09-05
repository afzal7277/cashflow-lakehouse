# Cashflow Lakehouse

A PySpark + Delta Lake lakehouse pipeline for financial transaction data, built and
validated locally on a sample, then scaled to a distributed Azure Databricks cluster
on a synthetically-expanded dataset (20-30GB+).

## Motive
Demonstrate large-scale batch optimization, ACID lakehouse architecture, and
ML-on-Spark — skills that go beyond streaming ETL (see: SWMS project).

## Problem
Raw data lakes lack ACID guarantees, naive Spark configs perform poorly at scale,
and ML features are often disconnected from the batch layer, causing staleness.

## Solution
A medallion (Bronze → Silver → Gold) pipeline on Delta Lake with benchmarked
performance optimization and a feature/scoring layer built directly on Gold.

## Architecture
Bronze (raw ingestion) → Silver (cleaned, MERGE upserts, schema evolution) →
Gold (aggregated metrics) → Benchmarks (naive vs optimized) → ML (feature eng +
MLlib model + batch scoring).

## Status
🚧 In progress

- [x] PRD / HLD / LLD documented
- [x] Dockerized dev environment (Python 3.11 + JDK 17)
- [x] Data generator — PaySim-seeded, scaled + perturbed, batched with logging/resume
- [x] Bronze layer — raw ingestion to Delta, ingestion metadata
- [x] Data quality tests (generator + Bronze)
- [x] Silver layer — cleaning, MERGE upserts, schema evolution (tested)
- [ ] Gold layer — aggregated metrics
- [ ] Benchmark module — naive vs optimized Spark config
- [ ] ML layer — feature engineering, MLlib model, batch scoring
- [ ] Azure Databricks + ADLS Gen2 cluster-scale run

## Setup
See `docs/local_setup.md`.