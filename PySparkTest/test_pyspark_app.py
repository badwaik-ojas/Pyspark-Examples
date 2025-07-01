import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark_app import process_data

@pytest.fixture(scope="module")
def spark():
    # Create SparkSession for testing
    spark = SparkSession.builder \
        .appName("PySpark Test") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

    yield spark

    # Stop SparkSession after tests
    spark.stop()

def test_process_data(spark, tmpdir):
    # Input data
    input_data = [
        ("Alice", 25),
        ("Bob", 35),
        ("Charlie", 45)
    ]
    input_path = "resources/input_data.csv"
    spark.createDataFrame(input_data, ["name", "age"]).write.csv(input_path, header=True)

    # Output path
    output_path = "resources/output_data"

    # Process data
    process_data(spark, input_path, str(output_path))

    # Check processed data
    processed_df = spark.read.csv(str(output_path), header=True, inferSchema=True)
    assert processed_df.count() == 2
    assert processed_df.filter(col("age") > 30).count() == 2
