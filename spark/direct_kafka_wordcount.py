#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Counts words in UTF8 encoded, '\n' delimited text directly received from Kafka in every 2 seconds.
 Usage: direct_kafka_wordcount.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      examples/src/main/python/streaming/direct_kafka_wordcount.py \
      localhost:9092 test`
"""

import sys

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from decimal import *
import datetime, uuid



def foreachTest(rdd):

    line = rdd[1]

    # rdd.saveAsTextFile("file:///home/peython/Documents/quizzler/spark/")

    # counts = rdd.flatMap(lambda line: line[1].split(" "))
    # for line in rdd:
    #     words = line.split(' ')
    #     print(words[0])
    # print(rdd)
    # with open("/home/peyton/Documents/quizzler/spark/test.txt", "a") as myfile:
    #     myfile.write(rdd[1])
    # rdd

    # rdd.map(lambda x: x[1])
    # rdd.pprint()
if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>")
        exit(-1)

    sc = SparkContext(appName="CassandraTest")
    # print sc.cassandraTable("oilgas","drill_data").collect()


    # sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    #
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    # rddLines = sc.parallelize(lines)
    # rddLines.pprint()

    kvs.foreachRDD(lambda rdd: rdd.foreach(foreachTest))
    # kvs.foreachRDD(foreachTest)
    # words = lines.flatMap(lambda line: line.split(" "))

    #
    # data = sc.parallelize(words)
    #
    # for line in data.collect():
    #     print line



    # data = {
    #     "drill_id":uuid.UUID('{d7e607ad-67b6-48c8-ab39-d56b5fc4c97d}'),
    #     "td":Decimal(split_data[0]),
    #     "bittd":Decimal(split_data[1]),
    #     "wob":Decimal(split_data[2]),
    #     "rpm":Decimal(split_data[3]),
    #     "rop":Decimal(split_data[4]),
    #     "flowin":Decimal(split_data[5]),
    #     "flowout":Decimal(split_data[6]),
    #     "plcirc":Decimal(split_data[7]),
    #     "mwin":Decimal(split_data[8]),
    #     "mwout":Decimal(split_data[9]),
    #     "data_timestamp":datetime.datetime.fromtimestamp(int(split_data[10])),
    #     "date":str(datetime.datetime.fromtimestamp(int(split_data[10])).day),
    # }


    # counts.pprint()

    ssc.start()
    ssc.awaitTermination()
