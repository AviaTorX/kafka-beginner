from kafka import KafkaConsumer
import logging
import json

logging.basicConfig(level=logging.INFO)

# properties for consumer
bootstrap_server:str = 'localhost:9092'
topic_read:str = 'third_topic'

# create consumer

consumer = KafkaConsumer(bootstrap_servers=bootstrap_server, group_id='my-group',
                        auto_offset_reset='earliest')

# read data from topic

consumer.subscribe(topic_read)

try:
    for message in consumer:
        logging.info(message.value)
except KeyboardInterrupt:
    '''
        it is to avoid annoying keybordInterrupt log in terminal
    '''
    logging.error('Consumer closed')