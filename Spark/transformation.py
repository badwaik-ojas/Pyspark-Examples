from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def clean_data(df: DataFrame) -> DataFrame:
    # Remove rows with null revenue
    df_clean = df.filter(col("revenue").isNotNull())
    # Convert revenue to integer
    df_clean = df_clean.withColumn("revenue", col("revenue").cast("int"))
    return df_clean
