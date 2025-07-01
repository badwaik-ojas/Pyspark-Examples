from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Change Cols Names").getOrCreate()

schema = StructType([
    StructField("order id", IntegerType(), True),
    StructField("product id", StringType(), True)
])

df = spark.createDataFrame([[1, "P123"], [2, "P234"]], schema=schema)
df.show()

df_schema = df.columns

for col in df_schema:
    df = df.withColumnRenamed(col, col.replace(" ","_"))

df.show()