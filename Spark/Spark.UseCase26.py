from pyspark.sql import SparkSession
from pyspark.sql.functions import when

# Create a SparkSession
spark = SparkSession.builder \
    .appName("DecodeFunctionExample") \
    .getOrCreate()

# Sample DataFrame
data = [("John", 25), ("Alice", 30), ("Bob", None)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Using when function to simulate decode
decoded_df = df.withColumn("age_group", when(df.age < 30, "Young").when(df.age > 30 & df.age <40 ,"In between").otherwise("Old"))

# Show DataFrame with decoded values
decoded_df.show()

# Stop SparkSession
spark.stop()
