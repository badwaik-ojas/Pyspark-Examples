# exceptAll

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ExceptExample") \
    .getOrCreate()

# Sample DataFrames creation
data1 = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
data2 = [("Alice", 25), ("David", 40)]
columns = ["Name", "Age"]
df1 = spark.createDataFrame(data1, columns)
df2 = spark.createDataFrame(data2, columns)

# Show the DataFrames
print("DataFrame 1:")
df1.show()
print("DataFrame 2:")
df2.show()

# Apply exceptAll() operation
result_df = df1.exceptAll(df2)

# Return a new DataFrame containing rows in this DataFrame but
# not in another DataFrame while preserving duplicates.
df1 = spark.createDataFrame( [("a", 1), ("a", 1), ("a", 1), ("a", 2), ("b",  3), ("c", 4)], ["C1", "C2"])
df2 = spark.createDataFrame([("a", 1), ("b", 3)], ["C1", "C2"])
df1.exceptAll(df2).show()

# Show the result DataFrame
print("Result DataFrame:")
result_df.show()

# Stop SparkSession
spark.stop()
