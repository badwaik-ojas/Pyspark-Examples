from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_data(spark, input_path, output_path):
    # Read input data
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Process data (example: filter rows where age > 30)
    processed_df = df.filter(col("age") > 30)

    # Write processed data to output path
    processed_df.write.csv(output_path, header=True, mode="overwrite")

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("PySpark App") \
        .getOrCreate()

    # Process data
    process_data(spark, "resources/input_data.csv", "resources/output_data")

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
