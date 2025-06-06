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
topic='app-logs'

log_levels=['INFO', 'ERROR', 'DEBUG']
services=['user_service', 'order-service', 'payment-service']
messages = {
    'user-service': [
        'User profile updated',
        'User registration completed',
        'User deletion failed'
    ],
    'order-service': [
        'Order created',
        'Order status updated',
        'Order cancellation failed'
    ],
    'payment-service': [
        'Payment processed',
        'Payment failed',
        'Refund issued'
    ]
}

try:
    while True:
        log_level = random.choice(log_levels)
        service = random.choice(list(messages.keys()))
        log = {
            'service_name': service,
            'message': random.choice(messages[service]),
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())
        }

        message = json.dumps(log)
        producer.produce(topic, key=service, value=message, headers=[('log_level', log_level.encode('utf-8'))])
        time.sleep(1)

except KeyboardInterrupt:
    producer.flush()
    sys.exit(0) 
