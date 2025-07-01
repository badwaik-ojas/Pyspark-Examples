# filters

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Filter Example") \
    .getOrCreate()

# Sample DataFrame creation
data = [("Alice, How are you?", 25), ("Bob, How are you?", 30), ("Charlie, How are you?", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
#df.show()

df = df.withColumn("Question", regexp_extract("Name", ",(.+)$", 1))
df.show()