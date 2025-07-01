# Using explode to expand the data which is in array for into additional rows

"""
The explode function in Apache Spark is used to split a column that contains an array or map type
into multiple rows, with one row for each element in the array or each key-value pair in the map. 
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder \
    .appName("Explode Example") \
    .getOrCreate()

# Sample data

# Array Type
data = [
    {"id": 1, "array_column": [1, 2, 3]},
    {"id": 2, "array_column": [4, 5]},
    {"id": 3, "array_column": [6]}
]

# Create DataFrame
df = spark.createDataFrame(data)

# using explode
exploded_df = df.select("id", explode("array_column").alias("exploded_col"))
exploded_df.show()

""" *********************************************************************** """

# Maptype
schema = StructType([
    StructField("id", StringType(), True),
    StructField("attributes", MapType(StringType(), StringType()), True)
])

# Sample data
data = [
    ("1", {"name": "John", "age": "30", "city": "New York"}),
    ("2", {"name": "Alice", "age": "25", "city": "San Francisco"})
]

df = spark.createDataFrame(data=data, schema=schema)
exploded_df = df.select("id", explode("attributes").alias("key", "value"))
exploded_df_1 = df.select("id", explode("attributes").alias("key", "value"))
exploded_df_1.show()
pivoted_df = exploded_df.groupBy("id").pivot("key").agg({"value": "first"})
pivoted_df.show(truncate=False)

df = df.withColumn("name", df["attributes"].getItem("name"))\
    .withColumn("age", df["attributes"].getItem("age"))\
    .withColumn("city", df["attributes"].getItem("city"))
df.show()


