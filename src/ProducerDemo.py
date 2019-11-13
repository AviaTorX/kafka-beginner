from kafka import KafkaProducer
import json

# create producer prooperties
bootstrap_server = 'localhost:9092'
topic = 'third_topic'
value_serializer = lambda m: json.dumps(m).encode('ascii')
key_serializer = lambda m: json.dumps(m).encode('ascii')

# create the producer
producer = KafkaProducer(bootstrap_servers=bootstrap_server, value_serializer=value_serializer)

# send data -> this is asynchronous because of this program exits and data never sent
producer.send('third_topic', {'key': 'valo helrd'})

#flush producer
producer.flush()
#flush and close producer
producer.close()