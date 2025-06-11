hdfs dfs -rm -r /odap /odap/checkpoint
hdfs dfs -mkdir /odap
echo "Complete reset HDFS: /odap"

rm -f ./Hadoop/last_push_timestamp.txt
touch ./Hadoop/last_push_timestamp.txt
echo "Complete reset file src/last_push_timestamp.txt"

docker exec -it kafka-docker-kafka-1 bash
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic credit_card_transactions
echo "Waiting for Kafka delete topic..."

sleep 3

kafka-topics.sh --bootstrap-server localhost:9092 --create --topic credit_card_transactions
echo "Created topic credit_card_transactions"