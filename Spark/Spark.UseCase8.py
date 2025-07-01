# Classic Word Count
"""
Use below to execute:
& C:/Users/Ojas/.conda/envs/Pyspark/python.exe d:/TestCode/Spark/Spark.UseCase8.py resources/filebasicswrite.txt
"""

import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(lambda x,y: x+y)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    schema = "word String, count Int"
    df = spark.createDataFrame(data=output, schema=schema).show()

    spark.stop()