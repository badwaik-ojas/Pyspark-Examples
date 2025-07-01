from pyspark.sql import SparkSession
from pyspark.sql.functions import array_intersect, col, expr

# Initialize Spark session
spark = SparkSession.builder.appName("CommonElementsCheck").getOrCreate()

# Sample data
data = [(["a", "b", "c"], ["c", "d", "e"]),
        (["x", "y", "z"], ["y", "z"]),
        (["e", "f", "g"], ["p", "q", "r"])]

# Create DataFrame
columns = ["Col1", "Col2"]
df = spark.createDataFrame(data, columns)

# Create a new Boolean column that checks if the two columns have common elements
df_with_common_check = df.withColumn("Has_Common_Elements", expr("size(array_intersect(Col1, Col2)) > 0"))

# Show the result
df_with_common_check.show(truncate=False)
