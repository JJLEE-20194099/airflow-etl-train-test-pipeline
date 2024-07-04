sudo /home/kafka/bin/kafka-delete-records.sh --bootstrap-server localhost:9092 --offset-json-file ./delete.json
sudo /home/kafka/bin/kafka-delete-records.sh --bootstrap-server localhost:9093 --offset-json-file ./delete.json
sudo /home/kafka/bin/kafka-delete-records.sh --bootstrap-server localhost:9094 --offset-json-file ./delete.json

sudo /home/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic raw_meeyland --delete
sudo /home/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --topic raw_meeyland --delete
sudo /home/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 --topic raw_meeyland --delete

