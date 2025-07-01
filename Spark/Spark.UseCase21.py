from pyspark.sql import SparkSession
from pyspark.sql.functions import schema_of_json, lit

# Create a SparkSession
spark = SparkSession.builder \
    .appName("SchemaOfJsonExample") \
    .getOrCreate()


df = spark.read.option("inferSchema",True).json("resources\complex.json")

schema = df.schema
print(schema)

df = spark.read.schema(schema).json("resources\complex.json")
df.show()

# Stop SparkSession
spark.stop()
