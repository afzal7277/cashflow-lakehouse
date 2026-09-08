"""
Data quality tests for the Gold layer (daily_metrics, fraud_summary,
top_accounts). Assumes gold/aggregate.py has already been run against the
real Silver table with default paths.

Run with: pytest tests/test_gold_quality.py
"""

import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from configs.spark_session import get_spark_session

SILVER_PATH = os.environ.get("SILVER_PATH", "data/silver/transactions")
DAILY_METRICS_PATH = os.environ.get("GOLD_DAILY_METRICS_PATH", "data/gold/daily_metrics")
FRAUD_SUMMARY_PATH = os.environ.get("GOLD_FRAUD_SUMMARY_PATH", "data/gold/fraud_summary")
TOP_ACCOUNTS_PATH = os.environ.get("GOLD_TOP_ACCOUNTS_PATH", "data/gold/top_accounts")
TOP_N_ACCOUNTS = int(os.environ.get("TOP_N_ACCOUNTS", 100))


@pytest.fixture(scope="module")
def spark():
    s = get_spark_session(app_name="cashflow-gold-tests")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def silver_df(spark):
    if not os.path.exists(SILVER_PATH):
        pytest.skip(f"{SILVER_PATH} not found — run silver/transform.py first.")
    return spark.read.format("delta").load(SILVER_PATH)


@pytest.fixture(scope="module")
def daily_metrics_df(spark):
    if not os.path.exists(DAILY_METRICS_PATH):
        pytest.skip(f"{DAILY_METRICS_PATH} not found — run gold/aggregate.py first.")
    return spark.read.format("delta").load(DAILY_METRICS_PATH)


@pytest.fixture(scope="module")
def fraud_summary_df(spark):
    if not os.path.exists(FRAUD_SUMMARY_PATH):
        pytest.skip(f"{FRAUD_SUMMARY_PATH} not found — run gold/aggregate.py first.")
    return spark.read.format("delta").load(FRAUD_SUMMARY_PATH)


@pytest.fixture(scope="module")
def top_accounts_df(spark):
    if not os.path.exists(TOP_ACCOUNTS_PATH):
        pytest.skip(f"{TOP_ACCOUNTS_PATH} not found — run gold/aggregate.py first.")
    return spark.read.format("delta").load(TOP_ACCOUNTS_PATH)


# --- daily_metrics ---

def test_daily_metrics_has_rows(daily_metrics_df):
    assert daily_metrics_df.count() > 0


def test_daily_metrics_transaction_count_matches_silver(daily_metrics_df, silver_df):
    total_from_gold = daily_metrics_df.agg({"transaction_count": "sum"}).collect()[0][0]
    total_from_silver = silver_df.count()
    assert total_from_gold == total_from_silver, (
        f"Gold daily_metrics total ({total_from_gold}) doesn't match "
        f"Silver row count ({total_from_silver}) — aggregation may be dropping rows."
    )


def test_daily_metrics_no_negative_amounts(daily_metrics_df):
    negative = daily_metrics_df.filter(daily_metrics_df["total_amount"] < 0).count()
    assert negative == 0


def test_daily_metrics_avg_consistent_with_total(daily_metrics_df):
    from pyspark.sql import functions as F
    inconsistent = daily_metrics_df.filter(
        F.abs(
            F.col("avg_amount") - (F.col("total_amount") / F.col("transaction_count"))
        ) > 0.01
    ).count()
    assert inconsistent == 0, "avg_amount doesn't match total_amount / transaction_count for some rows."


# --- fraud_summary ---

def test_fraud_summary_has_rows(fraud_summary_df):
    assert fraud_summary_df.count() > 0


def test_fraud_summary_fraud_count_matches_silver(fraud_summary_df, silver_df):
    total_fraud_gold = fraud_summary_df.agg({"fraud_count": "sum"}).collect()[0][0]
    total_fraud_silver = silver_df.filter(silver_df["isFraud"] == 1).count()
    assert total_fraud_gold == total_fraud_silver


def test_fraud_rate_within_valid_range(fraud_summary_df):
    out_of_range = fraud_summary_df.filter(
        (fraud_summary_df["fraud_rate"] < 0) | (fraud_summary_df["fraud_rate"] > 1)
    ).count()
    assert out_of_range == 0, "fraud_rate should always be between 0 and 1."


# --- top_accounts ---

def test_top_accounts_row_count_within_limit(top_accounts_df):
    assert top_accounts_df.count() <= TOP_N_ACCOUNTS


def test_top_accounts_no_negative_volume(top_accounts_df):
    negative = top_accounts_df.filter(top_accounts_df["total_volume"] < 0).count()
    assert negative == 0


def test_top_accounts_sorted_descending(top_accounts_df):
    rows = [r["total_volume"] for r in top_accounts_df.orderBy("total_volume", ascending=False).collect()]
    assert rows == sorted(rows, reverse=True), "top_accounts should be sorted descending by total_volume."