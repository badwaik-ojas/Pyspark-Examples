import pytest
from pyspark.sql import SparkSession
from Spark.transformation import clean_data

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest-pyspark").getOrCreate()

def test_schema_after_cleaning(spark):
    df = spark.createDataFrame([("East", "A", "100")], ["region", "product", "revenue"])
    result = clean_data(df)
    assert result.schema["revenue"].dataType.simpleString() == "int"

def test_null_revenue_filtered(spark):
    df = spark.createDataFrame([
        ("East", "A", "100"),
        ("West", "B", None)
    ], ["region", "product", "revenue"])
    result = clean_data(df)
    assert result.count() == 1

def test_data_integrity(spark):
    df = spark.createDataFrame([("East", "A", "100")], ["region", "product", "revenue"])
    result = clean_data(df)
    row = result.collect()[0]
    assert row["revenue"] == 100

def test_no_extra_columns(spark):
    df = spark.createDataFrame([("East", "A", "100")], ["region", "product", "revenue"])
    result = clean_data(df)
    assert set(result.columns) == {"region", "product", "revenue"}

def test_empty_input(spark):
    df = spark.createDataFrame([], schema="region STRING, product STRING, revenue STRING")
    result = clean_data(df)
    assert result.count() == 0
