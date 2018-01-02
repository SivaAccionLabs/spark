import sys
from pyspark import SparkContext


app_name = "Test-App"
sc = SparkContext(appName=app_name)
rdd1 = sc.textFile("hdfs:///user/deployment/app_packages/siva.txt")
rdd2 = rdd1.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
rdd2.collect()
rdd.count()

