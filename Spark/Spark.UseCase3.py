# write PySpark code to calculate the maximum value for each ID 
# within a sliding window of 3 hours

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, max

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Sliding Window Max Value") \
    .getOrCreate()

# Sample DataFrame creation (you would replace this with your actual data)
data = [(1, "2024-03-22 10:00:00", 10),
        (1, "2024-03-22 10:30:00", 20),
        (1, "2024-03-22 11:00:00", 15),
        (1, "2024-03-22 11:30:00", 25),
        (1, "2024-03-22 05:30:00", 25),
        (1, "2024-03-22 09:30:00", 25),
        (1, "2024-03-22 14:30:00", 25),
        (1, "2024-03-22 21:30:00", 50),
        (2, "2024-03-22 10:15:00", 30),
        (2, "2024-03-22 11:15:00", 40)]
df = spark.createDataFrame(data, ["id", "timestamp", "value"])

# Convert timestamp column to timestamp type
df = df.withColumn("timestamp", df["timestamp"].cast("timestamp"))

# Define sliding window
windowSpec = window("timestamp", "3 hours")

# Calculate maximum value within sliding window for each ID
max_value_df = df.groupBy("id", windowSpec).agg(max("value").alias("max_value"))

# Show the result
max_value_df.show(truncate=False)

# Stop the SparkSession
spark.stop()
