"""
Silver layer — cleaning, dedup, and ACID upserts.

Reads Bronze, drops nulls in key columns, casts types explicitly, computes a
synthetic transaction ID for dedup, and MERGE INTOs the Silver Delta table
(upsert: update existing txn IDs, insert new ones). This demonstrates ACID
guarantees that a plain Parquet/CSV lake doesn't provide.
"""

import logging
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from configs.spark_session import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from delta.tables import DeltaTable

BRONZE_PATH = os.environ.get("BRONZE_OUTPUT_PATH", "data/bronze/transactions")
SILVER_PATH = os.environ.get("SILVER_PATH", "data/silver/transactions")
LOG_DIR = os.environ.get("LOG_DIR", "logs")

KEY_COLUMNS = ["nameOrig", "amount", "type"]


def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("silver_transform")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(os.path.join(LOG_DIR, "silver_transform.log"), mode="a")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    return logger


def clean(df, logger):
    before = df.count()
    df = df.dropna(subset=KEY_COLUMNS)
    after = df.count()
    logger.info(f"Dropped {before - after} rows with nulls in key columns.")

    df = (
        df
        .withColumn("step", F.col("step").cast(IntegerType()))
        .withColumn("type", F.col("type").cast(StringType()))
        .withColumn("amount", F.col("amount").cast(DoubleType()))
        .withColumn("oldbalanceOrg", F.col("oldbalanceOrg").cast(DoubleType()))
        .withColumn("newbalanceOrig", F.col("newbalanceOrig").cast(DoubleType()))
        .withColumn("oldbalanceDest", F.col("oldbalanceDest").cast(DoubleType()))
        .withColumn("newbalanceDest", F.col("newbalanceDest").cast(DoubleType()))
        .withColumn("isFraud", F.col("isFraud").cast(IntegerType()))
        .withColumn("isFlaggedFraud", F.col("isFlaggedFraud").cast(IntegerType()))
    )
    return df


def add_transaction_id(df):
    """Deterministic hash of key fields — used as the MERGE key for dedup/upsert."""
    return df.withColumn(
        "transaction_id",
        F.sha2(
            F.concat_ws("||", F.col("nameOrig"), F.col("nameDest"), F.col("step"), F.col("amount")),
            256,
        ),
    )


def dedup(df, logger):
    before = df.count()
    df = df.dropDuplicates(["transaction_id"])
    after = df.count()
    logger.info(f"Removed {before - after} duplicate rows (by transaction_id).")
    return df


def merge_into_silver(spark, df, logger, silver_path=None):
    silver_path = silver_path or SILVER_PATH
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    if DeltaTable.isDeltaTable(spark, silver_path):
        logger.info("Silver table exists — running MERGE upsert.")
        silver_table = DeltaTable.forPath(spark, silver_path)
        (
            silver_table.alias("target")
            .merge(df.alias("source"), "target.transaction_id = source.transaction_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        logger.info("Silver table does not exist — creating it (first write).")
        df.write.format("delta").mode("overwrite").save(silver_path)


def main():
    logger = setup_logging()
    logger.info("=== Silver transform started ===")

    if not os.path.exists(BRONZE_PATH):
        raise FileNotFoundError(f"Bronze table not found at {BRONZE_PATH}. Run bronze/ingest.py first.")

    spark = get_spark_session(app_name="cashflow-silver-transform")

    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    logger.info(f"Read {bronze_df.count()} rows from Bronze.")

    cleaned = clean(bronze_df, logger)
    with_id = add_transaction_id(cleaned)
    deduped = dedup(with_id, logger)

    merge_into_silver(spark, deduped, logger)

    silver_count = spark.read.format("delta").load(SILVER_PATH).count()
    logger.info(f"Total rows now in Silver table: {silver_count}")
    logger.info("=== Silver transform complete ===")

    spark.stop()


if __name__ == "__main__":
    main()