from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Windows").getOrCreate()

df1 = spark.createDataFrame([[1],[2],[3],[4],[5]], schema="lis String")
df1.show()
person = [["Ojas",32],["Ojal",29]]
s = "name String, age Int"

df2 = spark.createDataFrame(person, schema=s)
df2.show()
df3 = df2.crossJoin(df1)
df3.cache()
df3.select("name", (col("age")*col("lis")).alias("result")).show()
df3.show()