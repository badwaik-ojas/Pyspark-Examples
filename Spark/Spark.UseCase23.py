from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("JoinExample") \
    .getOrCreate()

# Sample data for left DataFrame
left_data = [("John", "Doe", 28),
             ("Jane", "Smith", 35),
             ("Tom", "Brown", 40),
             ("Ojas","Badwaik", 35)]
left_df = spark.createDataFrame(left_data, ["first_name", "last_name", "age"])

# Sample data for right DataFrame
right_data = [("Doe", "Engineer"),
              ("Smith", "Doctor"),
              ("Brown", "Teacher")]
right_df = spark.createDataFrame(right_data, ["last_name", "profession"])

# Perform inner join
joined_df = left_df.join(right_df, "last_name", "inner")

# Show the result
joined_df.show()

# Perform left join
joined_df = left_df.join(right_df, "last_name", "left")

# Show the result
joined_df.show()

#############################################################

# Sample data for left DataFrame
left_data = [("John", "Doe", 28),
             ("Jane", "Smith", 35),
             ("Tom", "Brown", 40)]
left_df = spark.createDataFrame(left_data, ["first_name", "last_name", "age"])

# Sample data for right DataFrame
right_data = [("Doe", "Engineer", "USA"),
              ("Smith", "Doctor", "UK"),
              ("Brown", "Teacher", "Canada")]
right_df = spark.createDataFrame(right_data, ["surname", "profession", "country"])  # Different column names

# Perform inner join with different column names and multiple conditions
joined_df = left_df.join(right_df, (left_df["last_name"] == right_df["surname"]) & (left_df["age"] < 40), "inner")

# Show the result
joined_df.show()
