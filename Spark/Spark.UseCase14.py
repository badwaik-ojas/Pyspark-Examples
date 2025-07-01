from pyspark.sql import SparkSession

# Create a SparkContext
sc = SparkSession.builder.appName("RDD").getOrCreate().sparkContext

# Load the data into an RDD
lines_rdd = sc.textFile("resources/sales_data.csv")

# Split each line by comma and extract relevant fields
sales_rdd = lines_rdd.map(lambda line: line.split(","))

# Map each record to a tuple (Product, Price * Quantity)
revenue_rdd = sales_rdd.map(lambda fields: (fields[1], float(fields[2]) * int(fields[3])))

# Reduce by key to calculate total revenue per product
total_revenue_rdd = revenue_rdd.reduceByKey(lambda x, y: x + y)

# Map each record to a tuple (Product, Quantity)
quantity_rdd = sales_rdd.map(lambda fields: (fields[1], int(fields[3])))

# Reduce by key to calculate total quantity sold per product
total_quantity_rdd = quantity_rdd.reduceByKey(lambda x, y: x + y)

# Collect the results and print
revenue_results = total_revenue_rdd.collect()
quantity_results = total_quantity_rdd.collect()

print("Total Sales Revenue per Product:")
for product, revenue in revenue_results:
    print(f"{product}: ${revenue}")

print("\nTotal Quantity Sold per Product:")
for product, quantity in quantity_results:
    print(f"{product}: {quantity}")

# Stop the SparkContext
sc.stop()
