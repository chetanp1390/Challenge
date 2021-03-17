# Challenge
DDos Challenge


1) Start zookeeper

bin/windows/zookeeper-server-start.bat config/zookeeper.properties

2) Start Kafka

bin/windows/kafka-server-start.bat config/server.properties

3) Run ProducerDemo.scala to send logs messages to topic

4) Run Ddos.scala to check which IPs are trying to attack the server.
