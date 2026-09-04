"""
Bronze layer — raw ingestion.

Reads the generator's Parquet output, adds ingestion metadata, and writes
as a Delta table. Minimal transformation by design — Bronze preserves raw
data as-is (schema-on-read), cleaning happens in Silver.
"""

import logging
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from configs.spark_session import get_spark_session
from pyspark.sql import functions as F

INPUT_PATH = os.environ.get("BRONZE_INPUT_PATH", "data/raw/transactions")
OUTPUT_PATH = os.environ.get("BRONZE_OUTPUT_PATH", "data/bronze/transactions")
LOG_DIR = os.environ.get("LOG_DIR", "logs")


def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("bronze_ingest")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(os.path.join(LOG_DIR, "bronze_ingest.log"), mode="a")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    return logger


def main():
    logger = setup_logging()
    logger.info("=== Bronze ingestion started ===")
    logger.info(f"Input: {INPUT_PATH}  Output: {OUTPUT_PATH}")

    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(
            f"Input not found at {INPUT_PATH}. Run the generator first."
        )

    spark = get_spark_session(app_name="cashflow-bronze-ingest")

    df = spark.read.parquet(INPUT_PATH)
    input_count = df.count()
    logger.info(f"Read {input_count} rows from {INPUT_PATH}")

    bronze_df = (
        df
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit(INPUT_PATH))
    )

    bronze_df.write.format("delta").mode("append").save(OUTPUT_PATH)
    logger.info(f"Wrote {input_count} rows to Delta table at {OUTPUT_PATH}")

    total_count = spark.read.format("delta").load(OUTPUT_PATH).count()
    logger.info(f"Total rows now in Bronze table: {total_count}")
    logger.info("=== Bronze ingestion complete ===")

    spark.stop()


if __name__ == "__main__":
    main()