from kafka import SimpleProducer, KafkaClient
import time

kafka = KafkaClient('127.0.0.1:9998')
producer = SimpleProducer(kafka)

while True:
    producer.send_messages(b'test', str.encode("blah world"))
    time.sleep(5)