# udf

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,IntegerType

# Create SparkSession
spark = SparkSession.builder \
    .appName("UDF Example") \
    .getOrCreate()

# Sample DataFrame
data = [("John", 25), ("Alice", 30), ("Bob", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Define the UDF
def greet(name):
    return f"Hello, {name}!"

def add_1(age):
    return age+1

# Register the UDF
greet_udf = udf(greet, StringType())
add_udf = udf(add_1, IntegerType())

# Apply the UDF to a DataFrame column
df_with_greeting = df.withColumn("greeting", greet_udf(df["name"]))\
.withColumn("add_udf", add_udf(df["age"]))

# Show the DataFrame with the applied UDF
df_with_greeting.show()

# register UDF for sql
spark.udf.register("greet_udf", greet)
spark.udf.register("add_udf", add_1)
df.createOrReplaceTempView("temp")

spark.sql("select name, age, greet_udf(name) as greeting_sql, add_udf(age) as new_age from temp ").show()