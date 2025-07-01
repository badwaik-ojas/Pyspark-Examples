from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum

# Create a SparkSession
spark = SparkSession.builder \
    .appName("AggregationExample") \
    .getOrCreate()

# Sample data
data = [("John", 100),
        ("Jane", 200),
        ("Tom", 150)]

# Create a DataFrame
df = spark.createDataFrame(data, ["name", "score"])

# Calculate the average and sum of scores
agg_df = df.agg(avg("score").alias("average_score"), sum("score").alias("total_score"))
# df.select(df["name"].alias("test"))

# Show the result
agg_df.show()
