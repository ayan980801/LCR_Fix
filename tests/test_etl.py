import os
import sys
import hashlib
import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from ingest import add_hash_key


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield spark
    spark.stop()


def test_add_hash_key(spark):
    df = spark.createDataFrame([(1, "a")], ["id", "val"])
    metadata_cols = [
        "ETL_CREATED_DATE",
        "ETL_LAST_UPDATE_DATE",
        "CREATED_BY",
        "TO_PROCESS",
        "EDW_EXTERNAL_SOURCE_SYSTEM",
    ]
    result = add_hash_key(df, metadata_cols + ["HASH_COL"], "HASH_COL")
    expected_str = f"{df.first().id}||{df.first().val}"
    expected = hashlib.sha3_512(expected_str.encode("utf-8")).hexdigest()
    assert result.select("HASH_COL").first()[0] == expected


def test_anti_join_logic(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    metadata_cols = [
        "ETL_CREATED_DATE",
        "ETL_LAST_UPDATE_DATE",
        "CREATED_BY",
        "TO_PROCESS",
        "EDW_EXTERNAL_SOURCE_SYSTEM",
    ]
    df_hashed = add_hash_key(df, metadata_cols + ["HASH_COL"], "HASH_COL")
    target = df_hashed.limit(1)
    filtered = df_hashed.join(
        target.select("HASH_COL"),
        on="HASH_COL",
        how="left_anti",
    )
    remaining = [row.id for row in filtered.collect()]
    assert remaining == [2]
