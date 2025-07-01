#lag

"""
Write PySpark code to calculate the difference in value between the 
current row and the previous row, partitioned by id and ordered by date.
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Difference Between current and previous") \
    .getOrCreate()

# Sample DataFrame creation (you would replace this with your actual data)
data = [(1, "2024-03-22", 10),
        (1, "2024-03-23", 20),
        (1, "2024-03-24", 15),
        (2, "2024-03-22", 30),
        (2, "2024-03-23", 40),
        (2, "2024-03-24", 35)]
df = spark.createDataFrame(data, ["id", "date", "value"])

# Convert date column to date type
df = df.withColumn("date", df["date"].cast("date"))

# Define window partitioned by id and ordered by date
windowSpec = Window.partitionBy("id").orderBy(col("date").desc())

# Calculate the difference in value between the current row and the previous row

"""
We use the lag function to access the value of the previous row within each window partition 
and calculate the difference between the current row's value and the previous row's value.
"""
df_with_difference = df.withColumn("value_difference", lag("value", 1).over(windowSpec))

# Show the result
df_with_difference.show()



# Stop the SparkSession
spark.stop()
