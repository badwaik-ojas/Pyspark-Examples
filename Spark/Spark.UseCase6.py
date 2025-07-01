# Pivot example

# Pivot is used to Transform rows into columns, based on the distinct values in a specified column

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Windows").getOrCreate()
columns = "id Int, name String, deptname String, salary Int"
data = [ 
        [1,"ojas","Engineering",2000 ]
        ,[2,"ojal","Finance",4000 ]
        ,[3,"sharvari","HR",5000 ]
        ,[4,"sanket","Engineering",3000 ]
        ,[5,"shashank","Engineering",7000 ]
        ,[6,"priya","Marketing",2000 ]
        ,[7,"pranav","Marketing",3000 ]
        ,[8,"mayur","Marketing",9000 ]
        ,[9,"dhaval","HR",10000]
        ,[10,"ashish","HR",3000 ]
        ,[11,"kartik","Engineering",4000 ]
        ,[12,"chinmay","Finance",4000 ]
        ,[13,"ojas","Finance",10000 ]
        ,[14,"gaurav","HR",1000 ]
        ,[15,"kartik","Marketing",4000 ]
        ,[16,"ojas","Engineering",9000 ]
        ]

df = spark.createDataFrame(data=data, schema=columns)
df.groupBy("deptname").pivot("name").agg(sum("salary")).fillna(0).show()
