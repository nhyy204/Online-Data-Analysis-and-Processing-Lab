from confluent_kafka import Consumer, KafkaError
import time
import random
import sys
import select

consumer_config = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 'network-status-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['network-status'])

device_status = {}

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Reached end of partition')
            else:
                print(f'Error: {msg.error()}')
        else:
            data = msg.value().decode('utf-8')
            device, status = data.split(':')
            device_status[device] = status
            print(f"[{device}]: {status}")
except KeyboardInterrupt:
    print("\nConsumer dừng.")
finally:
    consumer.close()
    sys.exit(0)