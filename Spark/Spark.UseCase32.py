from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# Initialize Spark session
spark = SparkSession.builder.appName("FilterPriority").getOrCreate()

# Sample data
data = [("dm", "a", "C", 1),
        ("dm", "a", "B", 2),
        ("ex", "a", "A", 4),
        ("ex", "b", "B", 4),
        ("ex", "b", "X", 3),
        ("ex", "b", "Y", 2)]

# Create DataFrame
columns = ["Dataset_name", "Source_var", "Target_var", "priority"]
df = spark.createDataFrame(data, columns)

# Define window specification for partitioning and ordering
window_spec = Window.partitionBy("Dataset_name", "Source_var").orderBy(col("priority"))

# Add a row number based on the window specification
df_with_row_num = df.withColumn("row_number", row_number().over(window_spec))

# Filter the rows where row_number is 1 (i.e., the lowest priority)
result_df = df_with_row_num.filter(col("row_number") == 1).drop("row_number")

# Show the result
result_df.show()
