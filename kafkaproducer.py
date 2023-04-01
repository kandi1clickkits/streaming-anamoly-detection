from kafka import KafkaConsumer, KafkaProducer
import json
import time
from json import loads
import csv

bootstrap_servers = ['localhost:9092']
topicname = 'potential-drowning'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()

csvfile = open('test.csv', 'r')
fieldnames = ("HR","SpO2")
reader = csv.DictReader(csvfile, fieldnames)
header = next(reader)
if header != None:
    for row in reader:
        print(row)
        print(json.dumps(row))
        ack = producer.send(topicname, json.dumps(row).encode('utf-8'))
        time.sleep(1)
        metadata = ack.get()
        print(row)
        print("Data stream sent to topic : " + metadata.topic)