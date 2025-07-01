from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read JSON Example") \
    .getOrCreate()

# Define the path to the JSON file
json_file_path = "resources\data.text"

# Read the JSON file into a DataFrame
df = spark.read.json(json_file_path)

# Show the contents of the DataFrame
df.show()

sch = StructType().add("firstname",StringType(), True).add("age", IntegerType(), True)
df = spark.read.schema(sch).json(json_file_path)

# the firstname here would be null as the schema we provided and the schema in json did not matched
df.show()

# complex JSON

json_file_path = "resources\complex.json"
schema = StructType([
    StructField("person", StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("address", StructType([
            StructField("city", StringType(), True),
            StructField("zip", StringType(), True)
        ]), True),
        StructField("contacts", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("value", StringType(), True)
        ])), True)
    ]))
])

df = spark.read.schema(schema).json(json_file_path)
df.show(truncate=False)

df = df.select("person.name", "person.age", "person.address.city", "person.contacts", "person.contacts.type", "person.contacts.value")
df = df.withColumn("contacts", explode(col("contacts")))
df.select("name", "age", "city","contacts.type", "contacts.value").show()

