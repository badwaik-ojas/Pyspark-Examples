from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Initialize Spark session
spark = SparkSession.builder.appName("ProductAmountCountry").getOrCreate()

# Sample data
data = [("Banana", 1000, "USA"),
        ("Carrots", 1500, "USA"),
        ("Beans", 1600, "USA"),
        ("Orange", 2000, "USA"),
        ("Orange", 2000, "USA"),
        ("Banana", 400, "China"),
        ("Carrots", 1200, "China"),
        ("Beans", 1500, "China"),
        ("Orange", 4000, "China"),
        ("Banana", 2000, "Canada"),
        ("Carrots", 2000, "Canada"),
        ("Beans", 2000, "Mexico")]

# Create DataFrame
columns = ["Product", "Amount", "Country"]
df = spark.createDataFrame(data, columns)

# Pivot the DataFrame
pivot_df = df.groupBy("Product").pivot("Country").agg(sum("Amount"))

# Show the result
pivot_df.show()
