"""
Central Spark session builder.
Same function is used locally (small data) and later on Databricks (large data) —
only the environment changes, not the code, which is the whole point of the project.
"""

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session(app_name: str = "spark-finance-lakehouse", shuffle_partitions: int = 8) -> SparkSession:
    """
    Build a local SparkSession with Delta Lake support.

    shuffle_partitions is deliberately low (default 8) for local dev —
    Spark's default of 200 is tuned for clusters, not a laptop.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", "4g")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
