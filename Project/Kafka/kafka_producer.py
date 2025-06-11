from kafka import KafkaProducer
import csv, sys, os, json
from dotenv import load_dotenv, find_dotenv

# Load environment variables from a .env file
load_dotenv(find_dotenv())

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'default_topic')
DATA_FILE_PATH = os.getenv('DATA_FILE_PATH', '')

def read_data_from_csv(filepath):
    try:
        with open(filepath,'r') as file:
            reader=csv.DictReader(file)
            for row in reader:
                yield row
    except FileNotFoundError:
        raise FileNotFoundError(f"The path {filepath} does not exist!")
    except Exception as ex:
        raise Exception(f"Failed to read from csv: {ex}")

def create_kafka_producer(bootstrap_servers):
    try:
        producer =  KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        return producer
    except Exception as e:
        raise Exception(f"Failed to create Kafka producer: {e}")
    
def send(producer, topic, data):
    for transaction in data:
        try:
            producer.send(topic, value=transaction)
        except KeyboardInterrupt:
            print("Transaction sending process stopped.")
            raise
        except Exception as ex:
            raise Exception(f"Failed to send transaction: {ex}")
    producer.flush()

def stop_kafka(producer):
    try:
        producer.flush()
        producer.close()
    except Exception as ex:
        raise Exception(f"Failed to stop Kafka producer: {ex}")
    finally:
        sys.exit(0)

def main():
    try:
        print("Starting Kafka Producer...")

        producer=create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)

        transactions=read_data_from_csv(DATA_FILE_PATH)

        if transactions:
            send(producer, KAFKA_TOPIC, transactions)
        else:
            print("There are no transactions to send!")

    except KeyboardInterrupt:
        print("Stopped by user...")
    except Exception as ex:
        raise Exception(f"Error: {ex}")
    finally:
        print("Successfully sent transactions to Kafka topic")
        stop_kafka(producer)

if __name__=="__main__":
    main()