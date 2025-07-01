from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MultiplyNumbersWithColumn") \
    .getOrCreate()

# Sample list of numbers
numbers = [2, 3, 4, 5]

# Convert the list to a DataFrame
numbers_df = spark.createDataFrame([[num] for num in numbers], ["number"])

# Sample DataFrame with a column
data = [(1, "a"), (2, "b"), (3, "c")]
df = spark.createDataFrame(data, ["value", "column_to_multiply"])
df1 = df.select("*")
# Multiply each number from the list with the column in DataFrame
for num in numbers:
    column_name = f"multiplied_by_{num}"
    df1 = df1.withColumn(column_name, col("value") * num)

# Show the result
df1.show()

cross_df = df.crossJoin(numbers_df)
cross_df = cross_df.withColumn("multiplied", col("number")*col("value"))
cross_df.show()

# Stop the SparkSession
spark.stop()
