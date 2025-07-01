# Creating Dataframe using StructType and StructField

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("App Name: Basics").getOrCreate()

data = [["Ojas"]]

"""
Create Dataframe using:

  1. StructType
  2. Strings
  3. List

"""
# StructType

schema = StructType([
  StructField('fname', StringType(), True)
  ])

df = spark.createDataFrame(data=data, schema = schema)
df.show()

# String

schema = "fname String"
df = spark.createDataFrame(data=data, schema = schema)
df.show()

# List

schema = ["fname"]
df = spark.createDataFrame(data=data, schema = schema)
df.show()

# MapType 
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
df.select(df["id"],df["attributes.name"]).show()
df.show()



# Array Type:
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("array_column", ArrayType(IntegerType()), True)
])

data = [
    (1, [1, 2, 3]),
    (2, [4, 5]),
    (3, [6])
]

df = spark.createDataFrame(data=data, schema=schema)
df.show()
