import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, col

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from ingest import add_etl_change_tracking


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield spark
    spark.stop()


def test_add_etl_change_tracking(spark):
    df = spark.createDataFrame([(1, "a")], ["id", "val"])
    metadata_cols = [
        "ETL_CREATED_DATE",
        "ETL_LAST_UPDATE_DATE",
        "CREATED_BY",
        "TO_PROCESS",
        "EDW_EXTERNAL_SOURCE_SYSTEM",
        "ETL_CHANGE_TRACKING",
    ]
    result = add_etl_change_tracking(df, metadata_cols)
    expected = df.select(
        sha2(concat_ws("||", col("id").cast("string"), col("val")), 512).alias("hash")
    ).first()[0]
    assert result.select("ETL_CHANGE_TRACKING").first()[0] == expected


def test_anti_join_logic(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    metadata_cols = [
        "ETL_CREATED_DATE",
        "ETL_LAST_UPDATE_DATE",
        "CREATED_BY",
        "TO_PROCESS",
        "EDW_EXTERNAL_SOURCE_SYSTEM",
        "ETL_CHANGE_TRACKING",
    ]
    df_hashed = add_etl_change_tracking(df, metadata_cols)
    target = df_hashed.limit(1)
    filtered = df_hashed.join(
        target.select("ETL_CHANGE_TRACKING"),
        on="ETL_CHANGE_TRACKING",
        how="left_anti",
    )
    remaining = [row.id for row in filtered.collect()]
    assert remaining == [2]
