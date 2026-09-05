"""
Demonstrates and verifies Silver MERGE (update + insert) and schema
evolution, using a small isolated fixture table — NOT the main 5M-row
Silver table. Run with: pytest tests/test_silver_merge.py -v
"""

import os
import shutil
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from configs.spark_session import get_spark_session
from silver.transform import add_transaction_id, merge_into_silver
from pyspark.sql import Row

TEST_SILVER_PATH = "data/test_silver_merge_fixture"


@pytest.fixture(scope="module")
def spark():
    s = get_spark_session(app_name="cashflow-silver-merge-test")
    yield s
    s.stop()


@pytest.fixture(autouse=True)
def clean_fixture_path():
    # Ensure a clean slate before each test run of this module
    if os.path.exists(TEST_SILVER_PATH):
        shutil.rmtree(TEST_SILVER_PATH)
    yield
    if os.path.exists(TEST_SILVER_PATH):
        shutil.rmtree(TEST_SILVER_PATH)


def make_rows(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows])


def base_rows():
    return [
        {"nameOrig": "C1", "nameDest": "D1", "step": 1, "amount": 100.0, "type": "PAYMENT"},
        {"nameOrig": "C2", "nameDest": "D2", "step": 1, "amount": 200.0, "type": "TRANSFER"},
        {"nameOrig": "C3", "nameDest": "D3", "step": 1, "amount": 300.0, "type": "CASH_OUT"},
    ]


def test_merge_updates_existing_and_inserts_new(spark, tmp_path=None):
    # --- First write: 3 base rows, creates the table ---
    initial_df = add_transaction_id(make_rows(spark, base_rows()))
    merge_into_silver(spark, initial_df, logger=_NullLogger(), silver_path=TEST_SILVER_PATH)

    initial_count = spark.read.format("delta").load(TEST_SILVER_PATH).count()
    assert initial_count == 3

    # --- Second write: update one existing row's amount, add one new row ---
    updated_and_new = [
        {"nameOrig": "C1", "nameDest": "D1", "step": 1, "amount": 999.0, "type": "PAYMENT"},  # updated amount -> same transaction_id? No: amount is part of hash
        {"nameOrig": "C4", "nameDest": "D4", "step": 1, "amount": 400.0, "type": "DEBIT"},     # brand new row
    ]
    # NOTE: transaction_id is a hash of (nameOrig, nameDest, step, amount).
    # Changing amount changes the id, which would make it a new row rather
    # than an update. To demonstrate a true UPDATE, we must keep the key
    # fields identical and change a NON-key column instead.
    second_df = add_transaction_id(make_rows(spark, updated_and_new))
    merge_into_silver(spark, second_df, logger=_NullLogger(), silver_path=TEST_SILVER_PATH)

    result = spark.read.format("delta").load(TEST_SILVER_PATH)
    total = result.count()
    # C1's original id no longer exists (amount changed the hash), so this
    # becomes an INSERT, not an update — total should be 3 + 2 = 5.
    assert total == 5


def test_merge_true_update_on_non_key_column(spark):
    """
    A genuine UPDATE requires the transaction_id (hash of nameOrig, nameDest,
    step, amount) to stay the same, with a different non-key column value —
    e.g. re-ingesting the same transaction with a corrected isFraud flag.
    """
    row_v1 = [{"nameOrig": "C9", "nameDest": "D9", "step": 5, "amount": 50.0, "type": "PAYMENT", "isFraud": 0}]
    df_v1 = add_transaction_id(make_rows(spark, row_v1))
    merge_into_silver(spark, df_v1, logger=_NullLogger(), silver_path=TEST_SILVER_PATH)

    row_v2 = [{"nameOrig": "C9", "nameDest": "D9", "step": 5, "amount": 50.0, "type": "PAYMENT", "isFraud": 1}]
    df_v2 = add_transaction_id(make_rows(spark, row_v2))
    merge_into_silver(spark, df_v2, logger=_NullLogger(), silver_path=TEST_SILVER_PATH)

    result = spark.read.format("delta").load(TEST_SILVER_PATH)
    assert result.count() == 1, "Same transaction_id should update in place, not duplicate."
    assert result.collect()[0]["isFraud"] == 1, "isFraud should reflect the updated value."


def test_schema_evolution_adds_column_without_breaking(spark):
    """
    Simulates adding a new column ('channel') mid-project. With
    mergeSchema=true, the new column should appear, existing rows get
    null for it, and the merge doesn't fail.
    """
    df_v1 = add_transaction_id(make_rows(spark, base_rows()))
    merge_into_silver(spark, df_v1, logger=_NullLogger(), silver_path=TEST_SILVER_PATH)

    new_row = [{"nameOrig": "C5", "nameDest": "D5", "step": 2, "amount": 500.0, "type": "PAYMENT", "channel": "mobile_app"}]
    df_v2 = add_transaction_id(make_rows(spark, new_row))
    merge_into_silver(spark, df_v2, logger=_NullLogger(), silver_path=TEST_SILVER_PATH)

    result = spark.read.format("delta").load(TEST_SILVER_PATH)
    assert "channel" in result.columns, "New column should appear after schema evolution."

    old_row_channel = result.filter(result["nameOrig"] == "C1").collect()[0]["channel"]
    assert old_row_channel is None, "Pre-existing rows should get null for the new column."

    new_row_channel = result.filter(result["nameOrig"] == "C5").collect()[0]["channel"]
    assert new_row_channel == "mobile_app"


class _NullLogger:
    """Minimal no-op logger so merge_into_silver's logger.info() calls don't fail in tests."""
    def info(self, *args, **kwargs):
        pass