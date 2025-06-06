from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, split, explode

# Create a SparkSession to use Spark
spark = SparkSession.builder.appName("sparkstream").getOrCreate()

# Read streaming data from a socket source (host: localhost, port: 9999)
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Split each line into words by whitespace, creating an array of words
words = lines.select(split(col("value"), "\\s+").alias("word"))

# Explode the array of words into individual rows (strings representing numbers)
numbers = words.select(explode(col("word")).alias("number_str"))

# Cast the strings to integers, dropping any rows where casting fails (nulls)
integer_numbers = numbers.selectExpr("cast(number_str as int) as number").na.drop()

# Aggregate the sum of all numbers received so far in the stream
sum_df = integer_numbers.groupBy().agg(sum("number").alias("total_sum"))

# Define checkpoint directory for fault-tolerance and recovery
checkpointDir = "C:/checkpoint/"

# Write the aggregated sum to the console output every 1 second
streamingQuery = sum_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .trigger(processingTime="1 second") \
    .option("checkpointLocation", checkpointDir) \
    .start()

# Await termination to keep the streaming query running until manually stopped
streamingQuery.awaitTermination()
