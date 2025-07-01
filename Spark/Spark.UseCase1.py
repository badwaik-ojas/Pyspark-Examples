# Segregate the values

# Split and getItem

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").getOrCreate()

data = [("1","Broome 400 Broome St, New York, NY 10013, USA")
        ,("2","Bajiprabhu Nagar, Nagpur, 440010, India")]

df = spark.createDataFrame(data, ["id","address"])

split_col = split(df["address"],",")

df = df.withColumn('street', split_col.getItem(0))
df = df.withColumn('state', split_col.getItem(1))
df = df.withColumn('pin', split_col.getItem(2))
df = df.withColumn('country', split_col.getItem(3))

df = df.drop("address")
df.select("id", "street").withColumn("x", when(col("id")%2 == 0, "even").otherwise("odd")).show()

df.show()


