from confluent_kafka import Producer
import time
import random
import sys

producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}
producer = Producer(producer_config)

devices = ['Server', 'Router', 'Switch']
statuses = ['Online', 'Offline']

topic = 'network-status'

try:
    while True:
        device = random.choice(devices)
        status = random.choice(statuses)
        message = f"{device}:{status}"
        producer.produce(topic, key=device, value=message)
        time.sleep(random.uniform(0.5, 2))  
        
except KeyboardInterrupt:
    producer.flush()
    sys.exit(0)
