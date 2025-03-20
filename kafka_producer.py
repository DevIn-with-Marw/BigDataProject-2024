import json
import datetime
from kafka import KafkaProducer
import time

# 1.2-1.3. Kafka Setup and Topic Creation: Through Kafka Command Line
#cd /BigDataProject/kafka_2.13-3.8.0
#bin/zookeeper-server-start.sh config/zookeeper.properties
#bin/zookeeper-server-start.sh config/zookeeper.properties
#bin/kafka-topics.sh --create --topic vehicles-positions --bootstrap-server localhost:9092 --partitions 4

# 1.4. Kafka Producer Setup: Kafka Producer is responsible for sending the simulator's output data to the kafka cluster
#every 2 seconds, the producer sends a new batch of data to the cluster in localhost:9092 in json formats 
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], linger_ms=10000, batch_size=5000, value_serializer=lambda m: json.dumps(m).encode('utf-8'))
#set start time
start = datetime.datetime.now()
count=0 #sent records

with open('./out/simres.json', 'r') as file:
    data = json.load(file)


for id, item in data.items():
    del item['dn']
    tosend = ["name", "origin", "destination", "time", "link", "position", "spacing", "speed"]
    #filter only vehicles on link
    if (item['link'] != 'waiting_at_origin_node' and item['link'] != 'trip_end'):
        item = dict(zip(tosend, list(item.values())))
        #fix time value
        time_change = datetime.timedelta(seconds=item["time"])
        new_time = start.replace(microsecond=0) + time_change
        item["time"] = new_time.strftime('%Y-%m-%d %H:%M:%S')
        #message send
        if(count%5000 == 0):    #batch full
            time.sleep(2)       #2 seconds wait time before continuing execution
        producer.send('vehicle-positions', value=item)
        count+=1
