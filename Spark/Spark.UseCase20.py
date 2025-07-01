from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("explode").getOrCreate()

schema = "id String, str String"
data = [[1,"AB#BC#CD"], [2,"DE#EF"]]

df = spark.createDataFrame(data=data, schema=schema)
df.show()

df = df.withColumn("str", explode(split(col("str"), "#")))
df.show()