from confluent_kafka import Consumer, KafkaError
import json
import time
import sys

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'event-counter-group',  
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['event']) 

event_counts = {'click': 0, 'view': 0, 'purchase': 0}
last_time = time.time()
log_file = open('event_stats.log', 'a', encoding='utf-8')  

try:
    while True:
        now = time.time()
        if now - last_time >= 60: 
            log_message = f"Số lượng sự kiện trong 60 giây: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now))}"
            log_file.write(log_message + "\n")
            
            for event_type in ['click', 'view', 'purchase']:
                count_message = f"{event_type}: {event_counts.get(event_type, 0)}"
                log_file.write(count_message + "\n")
            
            log_file.flush()  
            event_counts = {'click': 0, 'view': 0, 'purchase': 0}  
            last_time = now
        
        msg = consumer.poll(1.0)
        if msg is None:
            continue
            
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
            
        event_data = json.loads(msg.value().decode('utf-8'))
        event_type = event_data.get('event_type')
        event_counts[event_type] += 1

except KeyboardInterrupt:
    print("\nConsumer dừng.")
finally:
    consumer.close()
    log_file.close()
    sys.exit(0)