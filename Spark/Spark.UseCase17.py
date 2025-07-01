from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("TrendingHashtags") \
    .getOrCreate()

# Sample JSON input
json_data = [
    {"id": 1, "text": "#epam wow hellos #hello #hello1"},
    {"id": 2, "text": "#epam #epam #hello #epam wow hellos #hello #hello1"}
]   

# Create DataFrame from JSON data
df = spark.createDataFrame(json_data, schema=["id","text"])


# Tokenize the text and explode to separate hashtags
hashtags_df = df.withColumn("hashtags", explode(split(col("text"), " "))) \
                .filter(col("hashtags").startswith("#")) \
                .select(col("hashtags"))

# Count occurrences of each hashtag
trending_hashtags = hashtags_df.groupBy("hashtags").count()

# Find the most trending hashtag
most_trending_hashtag = trending_hashtags.orderBy("count", ascending=False).first()["hashtags"]

print("Most trending hashtag:", most_trending_hashtag)


# Stop SparkSession
spark.stop()
