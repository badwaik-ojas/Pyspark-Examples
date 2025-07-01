from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

data = [["ojas"],["ojal"]]
schema = "name String"

df = spark.createDataFrame(data=data, schema=schema)

df.agg({"name":"sum"}).show()
df.show()
