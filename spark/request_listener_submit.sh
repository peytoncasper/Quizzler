dse spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.4.1,com.datastax.spark:spark-cassandra-connector_2.10:1.5.0-M3 \
      spark/direct_kafka_wordcount.py \
      localhost:9998 test