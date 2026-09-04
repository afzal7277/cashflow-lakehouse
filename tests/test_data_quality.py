"""
Data quality tests for the generator output and Bronze layer.

Run with: pytest tests/test_data_quality.py -v
Assumes the generator and bronze ingest have already been run at least once
with the default paths (data/raw/transactions, data/bronze/transactions).
"""

import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from configs.spark_session import get_spark_session

RAW_PATH = os.environ.get("BRONZE_INPUT_PATH", "data/raw/transactions")
BRONZE_PATH = os.environ.get("BRONZE_OUTPUT_PATH", "data/bronze/transactions")
EXPECTED_FRAUD_RATIO = float(os.environ.get("FRAUD_RATIO", 0.005))
FRAUD_RATIO_TOLERANCE = 0.002  # +/- 0.2 percentage points

KEY_COLUMNS = ["nameOrig", "nameDest", "amount", "type"]

EXPECTED_COLUMNS = {
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
}


@pytest.fixture(scope="module")
def spark():
    s = get_spark_session(app_name="cashflow-tests")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def raw_df(spark):
    if not os.path.exists(RAW_PATH):
        pytest.skip(f"{RAW_PATH} not found — run the generator first.")
    return spark.read.parquet(RAW_PATH)


@pytest.fixture(scope="module")
def bronze_df(spark):
    if not os.path.exists(BRONZE_PATH):
        pytest.skip(f"{BRONZE_PATH} not found — run bronze/ingest.py first.")
    return spark.read.format("delta").load(BRONZE_PATH)


# --- Raw / generator output checks ---

def test_raw_has_rows(raw_df):
    assert raw_df.count() > 0, "Generator output is empty."


def test_raw_schema_has_expected_columns(raw_df):
    actual_columns = set(raw_df.columns)
    missing = EXPECTED_COLUMNS - actual_columns
    assert not missing, f"Missing expected columns: {missing}"


def test_raw_no_nulls_in_key_columns(raw_df):
    for col in KEY_COLUMNS:
        null_count = raw_df.filter(raw_df[col].isNull()).count()
        assert null_count == 0, f"Found {null_count} nulls in column '{col}'."


def test_raw_amount_non_negative(raw_df):
    negative_count = raw_df.filter(raw_df["amount"] < 0).count()
    assert negative_count == 0, f"Found {negative_count} rows with negative amount."


def test_raw_fraud_ratio_within_tolerance(raw_df):
    total = raw_df.count()
    fraud_count = raw_df.filter(raw_df["isFraud"] == 1).count()
    actual_ratio = fraud_count / total
    diff = abs(actual_ratio - EXPECTED_FRAUD_RATIO)
    assert diff <= FRAUD_RATIO_TOLERANCE, (
        f"Fraud ratio {actual_ratio:.4f} is outside tolerance of "
        f"{EXPECTED_FRAUD_RATIO} +/- {FRAUD_RATIO_TOLERANCE}."
    )


def test_raw_valid_transaction_types(raw_df):
    valid_types = {"CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"}
    actual_types = {row["type"] for row in raw_df.select("type").distinct().collect()}
    invalid = actual_types - valid_types
    assert not invalid, f"Found unexpected transaction types: {invalid}"


# --- Bronze layer checks ---

def test_bronze_row_count_matches_raw(raw_df, bronze_df):
    # Bronze is append-only, so on a single run it should match raw count.
    # If bronze has been run multiple times, this may need adjusting.
    assert bronze_df.count() >= raw_df.count(), (
        "Bronze table has fewer rows than the raw source — ingestion may have failed."
    )


def test_bronze_has_ingestion_metadata(bronze_df):
    assert "ingest_timestamp" in bronze_df.columns
    assert "source_file" in bronze_df.columns


def test_bronze_no_null_ingest_timestamp(bronze_df):
    null_count = bronze_df.filter(bronze_df["ingest_timestamp"].isNull()).count()
    assert null_count == 0, f"Found {null_count} rows with null ingest_timestamp."