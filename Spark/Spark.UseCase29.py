
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.storagelevel import StorageLevel


spark = SparkSession.builder.appName("Windows").getOrCreate()
columns = "id Int, name String, deptname String, salary String"
data = [ 
        [1,"ojas","Engineering",2000 ]
        ,[2,"ojal",None,4000 ]
        ,[3,"sharvari","HR",5000 ]
        ,[4,None,"Engineering",None ]
        ,[None,None,None,None ]
        ]
df = spark.createDataFrame(data=data, schema=columns)
#df.dropna(subset=["name"]).show() # default is how=any
#print(df.dtypes)
#print(df.first())
#df.foreach(lambda x: print(x[1]))
#df.orderBy(col("salary").desc()).show()

df.write.save(path="resources/save/", format="csv", mode="append")
df.write.saveAsTable