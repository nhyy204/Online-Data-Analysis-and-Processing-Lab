from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, concat_ws, date_format, regexp_replace, when, lpad, split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from bs4 import BeautifulSoup
import requests, os
from dotenv import load_dotenv, find_dotenv

# Load environment variables from a .env file
load_dotenv(find_dotenv())

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'default_topic')
HDFS_CHECKPOINT = os.getenv('HDFS_CHECKPOINT', 'hdfs://localhost:9000/odap/checkpoint/')
HDFS_PATH = os.getenv('HDFS_PATH', 'hdfs://localhost:9000/odap/credit_card_transactions')
URL = os.getenv('EXCHANGE_RATE_API_URL', '')

def create_spark_session(app_name):
    try:
        spark = SparkSession.builder\
            .appName(app_name)\
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as ex:
        raise Exception(f"Failed to create Spark session: {ex}")

def read_from_kafka(spark, kafka_bootstrap_servers, kafka_topic):
    try:
        df = spark.readStream\
                        .format("kafka")\
                        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
                        .option("subscribe", kafka_topic)\
                        .option("startingOffsets", "earliest")\
                        .load()
        return df
    except Exception as ex:
        raise Exception(f"Failed to read from Kafka: {ex}")
    
def define_schema():
    return StructType([
        StructField("User", StringType()),
        StructField("Card", StringType()),
        StructField("Year", StringType()),
        StructField("Month", StringType()),
        StructField("Day", StringType()),
        StructField("Time", StringType()),
        StructField("Amount", StringType()),
        StructField("Use Chip", StringType()),
        StructField("Merchant Name", StringType()),
        StructField("Merchant City", StringType()),
        StructField("Merchant State", StringType()),
        StructField("Zip", StringType()),
        StructField("MCC", StringType()),
        StructField("Errors?", StringType()),
        StructField("Is Fraud?", StringType()),
    ])
    
def parse_data_from_kafka(df, schema):
    try:
        parsed_df = df.select(col("value").cast("string").alias("value")) \
                .select(from_json(col("value"), schema).alias("data")) \
                .select("data.*")
        return parsed_df
    except Exception as ex:
        raise Exception(f"Failed to parse data from Kafka: {ex}")

def get_current_exchange_rate(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')  
        value = soup.find('input', {'id': 'target-input'})

        if value:
            raw_value = value.get('value')  
            cleaned_value = raw_value.replace('.', '')  
            exchange_rate = float(cleaned_value) / 1000  
            return exchange_rate
        else:
            print(f"Element with id='target-input' not found in {url}")
            return 26000
    except Exception as ex:
        print(f"Failed to get current exchange rate from {url}")
        return 26000

def transform(data):
    try:
        exchange_rate = get_current_exchange_rate(URL)
        df = data.withColumn(
            "Timestamp",
            to_timestamp(concat_ws(" ", concat_ws("-", col("Year"), lpad(col("Month"), 2, "0"), lpad(col("Day"), 2, "0")), col("Time")), "yyyy-MM-dd HH:mm")
        ).withColumn(
            "Date",
            date_format(col("Timestamp"), "yyyy-MM-dd") 
        ).withColumn(
            "Hour",
            split(col("Time"), ":").getItem(0).cast("int")
        ).withColumn(
            "Amount_USD", 
            regexp_replace(col("Amount"), "[$,]", "").cast(DoubleType())
        ).withColumn(
            "Amount_VND",
            col("Amount_USD") * exchange_rate
        ).withColumn(
            "Transaction_Type",
            when (col("Amount_USD") > 0, "Credit")
            .when (col("Amount_USD") < 0, "Debit")
            .otherwise("Unknown") 
        )

        return df.select(
            "Timestamp",
            "User",
            "Card",
            "Date",
            "Year",
            "Month",
            "Day",
            "Hour",
            "Time",
            "Amount_USD",
            "Amount_VND",
            "Use Chip",
            "Merchant Name",
            "Merchant City",
            "Merchant State",
            "Zip",
            "MCC",
            "Errors?",
            "Is Fraud?",
            "Transaction_Type"
        )
    except Exception as ex:
        raise Exception(f"Failed to transform data: {ex}")
    
def streaming(data, checkpointDir, path):
    try:
        streamingQuery = data.writeStream \
                        .format("csv") \
                        .outputMode("append") \
                        .option("path", path) \
                        .option("checkpointLocation", checkpointDir) \
                        .option("header", "true") \
                        .start()
        
        return streamingQuery
    except Exception as ex:
        raise Exception(f"Failed to streaming: {ex}")

def stop_spark_session(spark):
    try:
        if spark:
            spark.stop()
        else:
            print("Spark session is already stopped or was never started.")
    except Exception as e:
        raise Exception(f"Failed to stop Spark session: {e}")

def main():
    try:
        spark = create_spark_session('pyspark_streaming')
        df = read_from_kafka(spark, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
        schema = define_schema()
        data = parse_data_from_kafka(df, schema)
        data = transform(data)
        hdfs_query = streaming(data, HDFS_CHECKPOINT, HDFS_PATH)
        hdfs_query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopped by user...")
    except Exception as ex:
        raise Exception(f"Error: {ex}")
    finally:
        if 'spark' in locals():
            stop_spark_session(spark)

if __name__ == '__main__':
    main()