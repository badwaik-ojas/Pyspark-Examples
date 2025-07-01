# Cube and dtypes   

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CubeExample") \
    .getOrCreate()

# Sample data
data = [("John", "A", 100),
        ("John", "B", 200),
        ("Jane", "A", 150),
        ("Jane", "B", 250)]

# Create a DataFrame
df = spark.createDataFrame(data, ["name", "category", "score"])

# Create a cube on columns "name" and "category"
cube_df = df.cube("name", "category").agg(avg("score").alias("total_score"))

# Show the result
cube_df.show()

column_types = df.dtypes
print(column_types)
df1 = df.first()
print("Hello ",df1["name"])


