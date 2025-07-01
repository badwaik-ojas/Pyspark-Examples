from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# isin, when-otherwise, isNull

# Create a SparkSession
spark = SparkSession.builder \
    .appName("JoinExample") \
    .getOrCreate()

# Sample data for left DataFrame
data = [(1,"John", "Doe", 28),
             (2,"Jane", "Smith", 35),
             (3,"Tom", "Brown", 40),
             (4,"Ojas","Badwaik", 35),
             (5,"Ojal",None, 30)]
df = spark.createDataFrame(data, ["id","first_name", "last_name", "age"])

df_isin = df.withColumn("isin", df["first_name"].isin("Ojas")) # returns true/false if the value is present
df_isin.show()

df_whnothwise = df.withColumn("w-o", when(df["first_name"] == "Ojas",True).otherwise(False))
df_whnothwise.show()

df_isnull = df.withColumn("isNull", isnull(df["last_name"]))
df_isnull.show()
