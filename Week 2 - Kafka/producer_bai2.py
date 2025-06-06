from confluent_kafka import Producer
import time
import random
import sys
import json

producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}
producer = Producer(producer_config)
topic = 'event'

event_types=['click', 'view', 'purchase']
NUM_PARTITIONS = 3

def choose_partition(key_str):
    if key_str == 'click':
        return 0
    elif key_str == 'view':
        return 1
    elif key_str == 'purchase':
        return 2
    return random.randint(0, NUM_PARTITIONS - 1)

try:
    while True:
        event_type = random.choice(event_types)
        event_data= {
            'user_id': random.randint(1, 1000),
            'item_id': random.randint(1, 10000),
            'event_type': event_type,
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())
        }

        message = json.dumps(event_data).encode('utf-8')
        partition = choose_partition(event_type)
        producer.produce(topic, partition=partition, key=event_type.encode('utf-8'), value=message)
        time.sleep(1)
        
except KeyboardInterrupt:
    producer.flush()
    sys.exit(0)