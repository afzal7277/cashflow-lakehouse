"""
Generates the working transaction dataset for Cashflow Lakehouse.

Approach (hybrid):
1. Load a real PaySim seed sample (download separately from Kaggle, place at
   data/seed/paysim.csv) to capture real transaction/fraud distributions.
2. Resample + perturb (jitter amounts, regenerate account IDs, shift steps)
   to scale up to NUM_ROWS while preserving realistic patterns.

Processes in batches so progress can be logged and a failed/interrupted run
can resume from the last completed batch instead of starting over.

Locally: NUM_ROWS ~5M (small, fast iteration).
On cluster: NUM_ROWS ~100M+ (same script, just change the env var).
"""

import json
import logging
import os
import sys
import time

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from configs.spark_session import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

NUM_ROWS = int(os.environ.get("NUM_ROWS", 5_000_000))
FRAUD_RATIO = float(os.environ.get("FRAUD_RATIO", 0.005))
SEED_PATH = os.environ.get("SEED_PATH", "data/seed/paysim.csv")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "data/raw/transactions")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 500_000))
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "logs/generator_checkpoint.json")
LOG_DIR = os.environ.get("LOG_DIR", "logs")
RANDOM_SEED = 42


def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, "generator.log")

    logger = logging.getLogger("generator")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    return logger


def load_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH, "r") as f:
            return json.load(f)
    return {"completed_batches": [], "num_rows": NUM_ROWS, "fraud_ratio": FRAUD_RATIO}


def save_checkpoint(checkpoint):
    os.makedirs(os.path.dirname(CHECKPOINT_PATH), exist_ok=True)
    with open(CHECKPOINT_PATH, "w") as f:
        json.dump(checkpoint, f)


def load_seed(spark):
    if not os.path.exists(SEED_PATH):
        raise FileNotFoundError(
            f"Seed file not found at {SEED_PATH}. "
            "Download PaySim from Kaggle (search 'PaySim1') and place the "
            "CSV there before running the generator."
        )
    return spark.read.csv(SEED_PATH, header=True, inferSchema=True)


def scale_up(seed_df, target_rows: int, batch_seed_offset: int):
    """
    Resample + perturb one batch. batch_seed_offset varies the random seed
    per batch so different batches don't produce identical perturbations.
    """
    seed_count = seed_df.count()
    fraction = max(1.0, target_rows / seed_count)
    s = RANDOM_SEED + batch_seed_offset

    scaled = seed_df.sample(withReplacement=True, fraction=fraction, seed=s)
    scaled = scaled.limit(target_rows)

    scaled = (
        scaled
        .withColumn(
            "amount",
            (F.col("amount") * (F.lit(1) + (F.rand(s) - 0.5) * 0.3)).cast(DoubleType()),
        )
        .withColumn("nameOrig", F.concat(F.lit("C"), (F.rand(s + 1) * 1_000_000_000).cast("long")))
        .withColumn("nameDest", F.concat(F.lit("C"), (F.rand(s + 2) * 1_000_000_000).cast("long")))
        .withColumn("step", (F.col("step") + (F.rand(s + 3) * 500).cast("int")))
        .withColumn("newbalanceOrig", F.greatest(F.col("oldbalanceOrg") - F.col("amount"), F.lit(0.0)))
        .withColumn("newbalanceDest", F.col("oldbalanceDest") + F.col("amount"))
    )
    return scaled


def inject_fraud_ratio(df, fraud_ratio: float, batch_seed_offset: int):
    s = RANDOM_SEED + batch_seed_offset + 4
    return df.withColumn(
        "isFraud",
        F.when(F.rand(s) < fraud_ratio, F.lit(1)).otherwise(F.lit(0)),
    )


def main():
    logger = setup_logging()
    logger.info("=== Generator run started ===")
    logger.info(f"NUM_ROWS={NUM_ROWS} FRAUD_RATIO={FRAUD_RATIO} BATCH_SIZE={BATCH_SIZE}")

    checkpoint = load_checkpoint()
    if checkpoint.get("num_rows") != NUM_ROWS or checkpoint.get("fraud_ratio") != FRAUD_RATIO:
        logger.warning(
            "Checkpoint params differ from current run config — starting fresh checkpoint."
        )
        checkpoint = {"completed_batches": [], "num_rows": NUM_ROWS, "fraud_ratio": FRAUD_RATIO}

    completed = set(checkpoint["completed_batches"])
    num_batches = (NUM_ROWS + BATCH_SIZE - 1) // BATCH_SIZE

    if completed:
        logger.info(f"Resuming: {len(completed)}/{num_batches} batches already completed.")

    spark = get_spark_session(app_name="cashflow-generator")
    seed_df = load_seed(spark)
    seed_df.cache()
    logger.info(f"Seed rows loaded: {seed_df.count()}")

    total_written = 0
    start_time = time.time()

    for batch_idx in range(num_batches):
        if batch_idx in completed:
            logger.info(f"Batch {batch_idx + 1}/{num_batches} already done, skipping.")
            continue

        rows_this_batch = min(BATCH_SIZE, NUM_ROWS - batch_idx * BATCH_SIZE)
        batch_start = time.time()
        logger.info(f"Batch {batch_idx + 1}/{num_batches}: generating {rows_this_batch} rows ...")

        try:
            scaled = scale_up(seed_df, rows_this_batch, batch_seed_offset=batch_idx * 10)
            final_batch = inject_fraud_ratio(scaled, FRAUD_RATIO, batch_seed_offset=batch_idx * 10)

            write_mode = "overwrite" if batch_idx == 0 and not completed else "append"
            final_batch.write.mode(write_mode).parquet(OUTPUT_PATH)

            elapsed = time.time() - batch_start
            total_written += rows_this_batch
            logger.info(
                f"Batch {batch_idx + 1}/{num_batches} done in {elapsed:.1f}s "
                f"({rows_this_batch} rows). Total written so far: {total_written}."
            )

            completed.add(batch_idx)
            checkpoint["completed_batches"] = sorted(completed)
            save_checkpoint(checkpoint)

        except Exception as e:
            logger.error(f"Batch {batch_idx + 1}/{num_batches} FAILED: {e}")
            logger.error("Checkpoint saved up to last successful batch. Re-run the script to resume.")
            spark.stop()
            raise

    total_elapsed = time.time() - start_time
    logger.info(f"=== Generator run complete in {total_elapsed:.1f}s ===")

    final_df = spark.read.parquet(OUTPUT_PATH)
    row_count = final_df.count()
    logger.info(f"Final row count in {OUTPUT_PATH}: {row_count}")
    final_df.groupBy("isFraud").count().show()

    spark.stop()


if __name__ == "__main__":
    main()