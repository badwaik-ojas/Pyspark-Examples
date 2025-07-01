# Ways to substitute, drop, replace null values in dataframe

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Windows").getOrCreate()
columns = "id Int, name String, deptname String, salary String"
data = [ 
        [1,"ojas","Engineering",2000 ]
        ,[2,"ojal",None,4000 ]
        ,[3,"sharvari","HR",5000 ]
        ,[4,None,"Engineering",None ]
        ,[None,None,None,None ]
        ]
"""
fill: This is to fill the null values
    - If no columns given, it will fill the null values in all columns with "String" provided.
    - Provide the columns in the form of dict, to fill that particular values with default one.
"""
df = spark.createDataFrame(data=data, schema=columns)
df.na.fill({"deptname": "NA"}).show()
df.na.fill("NA").show()

df.fillna({"deptname": "fillna"}).show()

"""
drop: this comes with parameter, this is used to drop the rows havin null values.
    * how{any, all}
        - any: drop when any column has null values
        - all: drop when all the column has null values
"""
df.na.drop(how="any", subset=["salary"]).show()
df.na.drop(how="all").show()
df.na.drop()

"""
replace: function is used to replace null or NaN values in a DataFrame with specified values.
"""
df.show()
df.na.replace(to_replace=["null"], value=["NA"], subset=["deptname"]).show()

"""
dropna: Returns a new DataFrame omitting rows with null values.
fillna: Replace null values, with string specified.
"""
df.dropna(how="all").show()