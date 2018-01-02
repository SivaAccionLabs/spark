from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

ssc = StreamingContext(sc, 5)
topics = ['avro.events']
kafka_params = {'metadata.broker.list': '10.0.1.208:9092'}
messages = KafkaUtils.createDirectStream(ssc, topics, kafka_params).repartition(1)
rdd2 = messages.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

