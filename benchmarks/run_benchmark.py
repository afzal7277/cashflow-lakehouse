"""
Benchmark module — naive vs optimized Spark config.

Scenario: join the full Silver transactions table (5M rows) against the
small Gold top_accounts table (100 rows) to flag transactions involving a
top account. This is a textbook broadcast-join case.

Naive run:  no broadcast (autoBroadcastJoinThreshold disabled), AQE off,
            default cluster-sized shuffle partitions (200).
Optimized run: explicit broadcast hint on the small table, AQE on,
            shuffle partitions tuned for local data size.

Captures wall-clock time and the actual physical join strategy used
(read from the query plan) as evidence, writes results to a CSV, and
renders a comparison chart.
"""

import csv
import logging
import os
import sys
import time

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from configs.spark_session import get_spark_session
from pyspark.sql import functions as F

SILVER_PATH = os.environ.get("SILVER_PATH", "data/silver/transactions")
TOP_ACCOUNTS_PATH = os.environ.get("GOLD_TOP_ACCOUNTS_PATH", "data/gold/top_accounts")
RESULTS_CSV = os.environ.get("BENCHMARK_RESULTS_CSV", "benchmarks/results.csv")
RESULTS_CHART = os.environ.get("BENCHMARK_RESULTS_CHART", "benchmarks/results.png")
LOG_DIR = os.environ.get("LOG_DIR", "logs")


def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("benchmark")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(os.path.join(LOG_DIR, "benchmark.log"), mode="a")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    return logger


def run_join(spark, use_broadcast: bool, logger, run_label: str):
    silver_df = spark.read.format("delta").load(SILVER_PATH)
    top_accounts_df = spark.read.format("delta").load(TOP_ACCOUNTS_PATH).select(
        F.col("nameOrig").alias("top_nameOrig")
    )

    if use_broadcast:
        top_accounts_df = F.broadcast(top_accounts_df)

    joined = silver_df.join(
        top_accounts_df, silver_df["nameOrig"] == top_accounts_df["top_nameOrig"], "inner"
    )

    plan = joined._jdf.queryExecution().executedPlan().toString()
    join_strategy = "BroadcastHashJoin" if "BroadcastHashJoin" in plan else (
        "SortMergeJoin" if "SortMergeJoin" in plan else "Other"
    )
    logger.info(f"[{run_label}] Physical join strategy detected: {join_strategy}")

    start = time.time()
    row_count = joined.count()
    elapsed = time.time() - start

    logger.info(f"[{run_label}] Join produced {row_count} matching rows in {elapsed:.2f}s.")
    return elapsed, row_count, join_strategy


def write_results(results, logger):
    os.makedirs(os.path.dirname(RESULTS_CSV), exist_ok=True)
    with open(RESULTS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["run_label", "aqe_enabled", "shuffle_partitions",
                                                 "join_strategy", "elapsed_seconds", "row_count"])
        writer.writeheader()
        for row in results:
            writer.writerow(row)
    logger.info(f"Results written to {RESULTS_CSV}")


def write_chart(results, logger):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels = [r["run_label"] for r in results]
    times = [r["elapsed_seconds"] for r in results]

    plt.figure(figsize=(6, 4))
    bars = plt.bar(labels, times, color=["#c0392b", "#27ae60"])
    plt.ylabel("Elapsed time (seconds)")
    plt.title("Naive vs Optimized Join Benchmark")
    for bar, t in zip(bars, times):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{t:.1f}s",
                  ha="center", va="bottom")
    plt.tight_layout()
    plt.savefig(RESULTS_CHART)
    logger.info(f"Chart written to {RESULTS_CHART}")


def main():
    logger = setup_logging()
    logger.info("=== Benchmark run started ===")

    if not os.path.exists(SILVER_PATH) or not os.path.exists(TOP_ACCOUNTS_PATH):
        raise FileNotFoundError(
            "Silver and Gold top_accounts tables must both exist. "
            "Run silver/transform.py and gold/aggregate.py first."
        )

    results = []

    # --- Naive run: no broadcast, AQE off, default cluster shuffle partitions ---
    logger.info("Running NAIVE config: no broadcast, AQE off, shuffle_partitions=200 ...")
    spark = get_spark_session(
        app_name="cashflow-benchmark-naive",
        shuffle_partitions=200,
        aqe_enabled=False,
        broadcast_threshold_bytes=-1,  # disable auto-broadcast entirely
    )
    elapsed, row_count, strategy = run_join(spark, use_broadcast=False, logger=logger, run_label="naive")
    results.append({
        "run_label": "naive", "aqe_enabled": False, "shuffle_partitions": 200,
        "join_strategy": strategy, "elapsed_seconds": round(elapsed, 2), "row_count": row_count,
    })
    spark.stop()

    # --- Optimized run: explicit broadcast, AQE on, tuned shuffle partitions ---
    logger.info("Running OPTIMIZED config: broadcast hint, AQE on, shuffle_partitions=8 ...")
    spark = get_spark_session(
        app_name="cashflow-benchmark-optimized",
        shuffle_partitions=8,
        aqe_enabled=True,
    )
    elapsed, row_count, strategy = run_join(spark, use_broadcast=True, logger=logger, run_label="optimized")
    results.append({
        "run_label": "optimized", "aqe_enabled": True, "shuffle_partitions": 8,
        "join_strategy": strategy, "elapsed_seconds": round(elapsed, 2), "row_count": row_count,
    })
    spark.stop()

    write_results(results, logger)
    write_chart(results, logger)

    naive_time = results[0]["elapsed_seconds"]
    opt_time = results[1]["elapsed_seconds"]
    speedup = naive_time / opt_time if opt_time > 0 else float("inf")
    logger.info(f"Speedup: {speedup:.2f}x (naive {naive_time}s -> optimized {opt_time}s)")
    logger.info("=== Benchmark run complete ===")


if __name__ == "__main__":
    main()