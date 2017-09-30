# Kafka_poc

What I would recommend is to spin up a small 4 CPU 16GB CentOS 7.2 VM on Azure. I can give you access if you need it.

Install MySQL 5, Confluent 3.2.0 (gives you Kafka Brokers, Zookeeper and Schema Registry), 

Logstash 5.1.2, ElasticSearch 5.1.2.

Then create a small app that makes changes to a database and that gets published in Kafka topics. --- source

From there you can perform joins with KTables and KStreams to create a state store that can be queried through a Java REST API. 

These changes are then pushed back to a brand new topic that gets push to ElasticSearch indices through log stash. 

/bin/zookeeper-server-start.sh -daemon config/zookeeper.properties


**************************************************************************************************************
mysql | kafka (with or without stream processing) | logstash | elastic search


zookeeper
kafka broker
schema registry

kafka connect - jdbc connector(mysql)

**************************************************************************************************************
./bin/zookeeper-server-start -daemon ./etc/kafka/zookeeper.properties

./bin/kafka-server-start -daemon ./etc/kafka/server.properties

./bin/schema-registry-start -daemon ./etc/schema-registry/schema-registry.properties

./bin/connect-standalone -daemon etc/schema-registry/connect-avro-standalone.properties etc/kafka-connect-jdbc/source-mysql-connector.properties

mysql -u root -p 

./bin/kafka-avro-console-consumer --new-consumer --bootstrap-server localhost:9092 --topic test-mysql-jdbc-accounts --from-beginning

INSERT INTO accounts(name) VALUES('vamshi');

**************************************************************************************************************
Kstream example
**************************************************************************************************************
spark-submit --master yarn-client --queue ndxmvp --conf "spark.driver.memory=16G" --conf "spark.executor.memory=8G" --conf "spark.executor.cores=6" --conf "spark.executor.instances=25" --class io.confluent.examples.streams.MapFunctionScalaExample

kstream commands:
sudo bin/kafka-topics --create --topic TextLinesTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1
sudo bin/kafka-topics --create --topic UppercasedTextLinesTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1
sudo bin/kafka-topics --create --topic OriginalAndUppercasedTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1

java -cp target/streams-examples-3.2.0-standalone.jar io.confluent.examples.streams.MapFunctionScalaExample

bin/kafka-console-producer --broker-list localhost:9092 --topic TextLinesTopic	

bin/kafka-console-consumer --new-consumer --bootstrap-server localhost:9092 --topic UppercasedTextLinesTopic --from-beginning
bin/kafka-console-consumer --new-consumer --bootstrap-server localhost:9092 --topic OriginalAndUppercasedTopic --from-beginning

**************************************************************************************************************
logstash
**************************************************************************************************************
LOGSTASH
config files directory : /etc/logstash/conf.d
exe location    : /usr/share/logstash/bin 

sample command1 : bin/logstash -e 'input { stdin { } } output { stdout {} }'
sample command2 : sudo ./bin/logstash -f /etc/logstash/conf.d/kafka-stdout.conf

			input
			{
			  kafka
			  {
				topics => ['test-mysql-jdbc-accounts']
			  }
			}
			output
			{
			  stdout {}
			}

kafka-es: 
			input
			{
			  kafka
			  {
				topics => ['test-mysql-jdbc-accounts']
			  }
			}
			output
			{
			  elasticsearch { hosts => ["localhost:9200"] }
			}

to list the indices:
curl localhost:9200/_cat/indices?v			

**************************************************************************************************************
elastic search
**************************************************************************************************************

### NOT starting on installation, please execute the following statements to configure elasticsearch service to start automatically using systemd
 sudo systemctl daemon-reload
 sudo systemctl enable elasticsearch.service
### You can start elasticsearch service by executing
 sudo systemctl start elasticsearch.service
