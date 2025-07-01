from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Sample JSON data received dynamically
json_data = {  "name": "John",   "age": 30,  "address": {    "street": "123 Main St",       "city": "Anytown",       "zip": "12345"    },    "emails": ["john@example.com", "john.doe@example.com"]}

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Create Dynamic Schema") \
    .getOrCreate()

# Define a function to recursively parse the JSON data and create the schema
def create_schema(json_data):
    fields = []
    print("**** ", json_data)
    for key, value in json_data.items():
        if isinstance(value, dict):
            fields.append(StructField(key, create_schema(value)))
        elif isinstance(value, list):
            element_type = StringType()
            fields.append(StructField(key, ArrayType(element_type), nullable=True))
        elif isinstance(value, int):
            fields.append(StructField(key, IntegerType(), nullable=True))
        else:
            fields.append(StructField(key, StringType(), nullable=True))
    return StructType(fields)

# Create the schema dynamically
schema = create_schema(json_data)

# Load JSON data with the dynamically created schema
df = spark.createDataFrame(data=[json_data], schema=schema)

# Show the DataFrame
df.show()

# Stop the SparkSession
spark.stop()
