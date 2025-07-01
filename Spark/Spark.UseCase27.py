from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Decorator Example") \
    .getOrCreate()

# Define a decorator function to log function calls
def log_function_call(func):
    def test(*args, **kwargs):
        print(f"Calling function '{func.__name__}'")
        result = func(*args, **kwargs)
        print(f"After Calling function '{func.__name__}'")
        return result
    return test

# Apply the decorator to a PySpark DataFrame transformation function
@log_function_call
def process_data(df):
    # Perform some data transformation
    df_processed = df.filter(col("age") > 18)
    return df_processed

# Load sample data into a DataFrame
data = [(1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 17)]
columns = ["id", "name", "age"]
df = spark.createDataFrame(data, columns)

# Call the decorated function
processed_df = process_data(df)

# Show the processed DataFrame
processed_df.show()

# Stop the SparkSession
spark.stop()
