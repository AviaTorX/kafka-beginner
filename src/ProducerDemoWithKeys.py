from kafka import KafkaProducer
import json
import logging

logging.basicConfig(level=logging.INFO)

# create producer prooperties
bootstrap_server = 'localhost:9092'
topic = 'third_topic'
value_serializer = lambda m: json.dumps(m).encode('ascii')
key_serializer = lambda m: json.dumps(m).encode('ascii')

# create the producer
producer = KafkaProducer(bootstrap_servers=bootstrap_server, value_serializer=value_serializer, key_serializer=lambda m: str(m).encode('ascii'))

# callback definations


def on_send_success(record_meradata):
    logging.info(record_meradata.topic)
    logging.info(record_meradata.partition)
    logging.info(record_meradata.offset)

def on_send_error(excp):
    logging.error(excp)

# send data -> this is asynchronous because of this program exits and data never sent
for i in range(10):
    topic: str = 'third_topic'
    data: any = {'key': 'to world no. '+str(i)}
    key_producer: str = 'id_'+str(i)
    '''
     Though key parameter takes byte type value we will be passing type str and our
     key_serializer will take care of parsing key into bytes
    '''
    producer.send(topic, data, key=key_producer).add_callback(on_send_success).add_errback(on_send_error)

#flush producer
producer.flush()
#flush and close producer
producer.close()