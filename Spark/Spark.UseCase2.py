# aggregation, group by and filter

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, sum

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Total Revenue per Customer") \
    .getOrCreate()

# Read the dataset
orders_df = spark.read.csv("resources\orders.csv", header=True, inferSchema=True)

# Filter orders for the year 2024
orders_2024_df = orders_df.filter(year("order_date") == 2024)

# Calculate total revenue per customer for the year 2024
revenue_per_customer_2024_df = orders_2024_df.groupBy("customer_id") \
    .agg(sum("order_amount").alias("total_revenue_2024"))

# agg({"<columnName>": "<aggregation>"})

# Show the result
revenue_per_customer_2024_df.show()

# Stop the SparkSession
spark.stop()
