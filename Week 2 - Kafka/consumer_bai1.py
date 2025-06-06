from confluent_kafka import Consumer, KafkaError
import time

consumer_config = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 'error_group', 
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['app-logs'])

error_log=open('error.log', 'a', encoding='utf-8')
log_counts={'INFO': 0, 'ERROR': 0, 'DEBUG': 0} 
last_time = time.time()
error_queue = []

try:
    while True:
        now = time.time()
        
        if now - last_time >= 10:
            print("\nSố lượng log sau 10s:")
            for level in ['INFO', 'ERROR', 'DEBUG']:
                print(f"{level}: {log_counts.get(level, 0)}")
            
            log_counts = {'INFO': 0, 'ERROR': 0, 'DEBUG': 0}
            last_time = now

        msg = consumer.poll(1.0)  
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        log_level = None
        for key, val in msg.headers() or []:
            if key == 'log_level':
                log_level = val.decode('utf-8')

        log_counts[log_level] +=1

        if log_level == 'ERROR':
            error_log.write(msg.value().decode('utf-8') + '\n')
            error_log.flush()
            error_queue.append(msg.value().decode('utf-8'))
except KeyboardInterrupt:
    print("\nConsumer dừng.")
finally:
    consumer.close()
    error_log.close()
