# filters

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Filter Example") \
    .getOrCreate()

# Sample DataFrame creation
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Applying filters
# Filter rows where Age is greater than 25
filtered_df = df.filter(df["Age"] > 25)
filtered_df.show()

# Filter rows where Name is "Alice"
filtered_df = df.filter(df["Name"] == "Alice")
filtered_df.show()

# Filter rows based on multiple conditions
filtered_df = df.filter((df["Age"] > 25) & (df["Name"] != "Charlie"))
filtered_df.show()

filtered_df = df.filter("Name = 'Alice'")
filtered_df.show()

# Stop SparkSession
spark.stop()


