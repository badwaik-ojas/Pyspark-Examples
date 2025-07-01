from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MultiplyWithList") \
    .getOrCreate()

# Sample DataFrame
data = [("A", 2), ("B", 3), ("C", 4)]
df = spark.createDataFrame(data, ["column1", "column2"])

# Sample list of numbers
number_list = [10, 20, 30]

# Broadcast the list of numbers to all the worker nodes
broadcast_list = spark.sparkContext.broadcast(number_list)

# Define a UDF to multiply each number with the column value
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

@udf(IntegerType())
def multiply_with_list(value):
    numbers = broadcast_list.value
    return [value * num for num in numbers]

# Apply the UDF to the DataFrame column

df_with_multiplied_column = df.withColumn("multiplied_column", lit(multiply_with_list(col("column2"))))

# Show the result
df_with_multiplied_column.show()

# Stop SparkSession
spark.stop()
