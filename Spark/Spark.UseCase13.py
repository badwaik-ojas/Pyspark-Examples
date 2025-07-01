# Rename Columns
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Column Rename") \
    .getOrCreate()

# Sample DataFrames creation
data1 = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]
df1 = spark.createDataFrame(data1, columns)
df1.show()
df2 = df1.withColumnRenamed("name", "firstname")
df2.show()

