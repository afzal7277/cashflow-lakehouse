"""
Gold layer — aggregated business metrics.

Reads Silver, computes daily transaction volume/amount by type, fraud rate
by type/day, and top accounts by volume. Writes partitioned Delta tables
and Z-orders by 'type' for query performance.

PaySim's 'step' is hours-since-start (no real calendar dates), so we derive
a synthetic day bucket: day_bucket = step // 24.
"""

import logging
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from configs.spark_session import get_spark_session
from pyspark.sql import functions as F
from delta.tables import DeltaTable

SILVER_PATH = os.environ.get("SILVER_PATH", "data/silver/transactions")
GOLD_DAILY_METRICS_PATH = os.environ.get("GOLD_DAILY_METRICS_PATH", "data/gold/daily_metrics")
GOLD_FRAUD_SUMMARY_PATH = os.environ.get("GOLD_FRAUD_SUMMARY_PATH", "data/gold/fraud_summary")
GOLD_TOP_ACCOUNTS_PATH = os.environ.get("GOLD_TOP_ACCOUNTS_PATH", "data/gold/top_accounts")
LOG_DIR = os.environ.get("LOG_DIR", "logs")
TOP_N_ACCOUNTS = int(os.environ.get("TOP_N_ACCOUNTS", 100))


def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("gold_aggregate")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(os.path.join(LOG_DIR, "gold_aggregate.log"), mode="a")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    return logger


def add_day_bucket(df):
    return df.withColumn("day_bucket", (F.col("step") / F.lit(24)).cast("int"))


def build_daily_metrics(df):
    return (
        df.groupBy("day_bucket", "type")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
        )
        .orderBy("day_bucket", "type")
    )


def build_fraud_summary(df):
    return (
        df.groupBy("day_bucket", "type")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("isFraud").alias("fraud_count"),
        )
        .withColumn("fraud_rate", F.col("fraud_count") / F.col("transaction_count"))
        .orderBy("day_bucket", "type")
    )


def build_top_accounts(df, account_col, top_n):
    return (
        df.groupBy(account_col)
        .agg(F.sum("amount").alias("total_volume"), F.count("*").alias("transaction_count"))
        .orderBy(F.desc("total_volume"))
        .limit(top_n)
    )


def write_and_zorder(spark, df, path, partition_col, zorder_col, logger, table_name):
    df.write.format("delta").mode("overwrite").partitionBy(partition_col).save(path)
    row_count = df.count()
    logger.info(f"Wrote {row_count} rows to {table_name} at {path} (partitioned by {partition_col}).")

    delta_table = DeltaTable.forPath(spark, path)
    delta_table.optimize().executeZOrderBy(zorder_col)
    logger.info(f"Z-ordered {table_name} by '{zorder_col}'.")


def main():
    logger = setup_logging()
    logger.info("=== Gold aggregation started ===")

    if not os.path.exists(SILVER_PATH):
        raise FileNotFoundError(f"Silver table not found at {SILVER_PATH}. Run silver/transform.py first.")

    spark = get_spark_session(app_name="cashflow-gold-aggregate")

    silver_df = spark.read.format("delta").load(SILVER_PATH)
    logger.info(f"Read {silver_df.count()} rows from Silver.")

    with_bucket = add_day_bucket(silver_df)
    with_bucket.cache()

    logger.info("Building daily_metrics ...")
    daily_metrics = build_daily_metrics(with_bucket)
    write_and_zorder(spark, daily_metrics, GOLD_DAILY_METRICS_PATH, "day_bucket", "type", logger, "daily_metrics")

    logger.info("Building fraud_summary ...")
    fraud_summary = build_fraud_summary(with_bucket)
    write_and_zorder(spark, fraud_summary, GOLD_FRAUD_SUMMARY_PATH, "day_bucket", "type", logger, "fraud_summary")

    logger.info(f"Building top_accounts (top {TOP_N_ACCOUNTS} by origin volume) ...")
    top_origin = build_top_accounts(with_bucket, "nameOrig", TOP_N_ACCOUNTS)
    top_origin.write.format("delta").mode("overwrite").save(GOLD_TOP_ACCOUNTS_PATH)
    logger.info(f"Wrote {top_origin.count()} rows to top_accounts at {GOLD_TOP_ACCOUNTS_PATH}.")

    logger.info("=== Gold aggregation complete ===")
    spark.stop()


if __name__ == "__main__":
    main()