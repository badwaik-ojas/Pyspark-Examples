# date function

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_sub,months_between,lit,date_format,current_date, current_timestamp, date_add, datediff, year, month, to_date

# Create SparkSession
spark = SparkSession.builder \
    .appName("Date Functions Example") \
    .getOrCreate()

# Sample data
data = [(1, "2020-03-20"), (2, "2024-03-25"), (3, "2024-04-01")]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "date_str"])

# Convert string to date
df = df.withColumn("date", to_date("date_str"))

# Add current date and timestamp columns
df = df.withColumn("current_date", current_date()).withColumn("current_timestamp", current_timestamp())

# Add 7 days to date
df = df.withColumn("date_plus_7_days", date_add("date", 7))

# Calculate the difference in days between two dates
df = df.withColumn("days_since_today", datediff(current_date(), "date"))

# Extract year and month from date
df = df.withColumn("year", year("date")).withColumn("month", month("date"))

# Show DataFrame
df.show()

df1 = spark.range(1).select(
    date_format(to_date(lit("2024-03-23")), "MM/dd/yyyy").alias("formatted_date")
)

# Show DataFrame
df1.show()

df = spark.range(1).select(
    to_date(lit("2024-03-23")).alias("input_date"),
    date_add(to_date(lit("2024-03-23")), 7).alias("date_after_7_days"),
    date_sub(to_date(lit("2024-03-23")), 7).alias("date_before_7_days")
)

# Calculate the difference in days between two dates
df = df.withColumn("days_difference", datediff(to_date(lit("2024-03-23")), to_date(lit("2024-03-16"))))

# Calculate the difference in months between two dates
df = df.withColumn("months_difference", months_between(to_date(lit("2024-03-23")), to_date(lit("2024-01-23"))))

df.show(truncate=False)


