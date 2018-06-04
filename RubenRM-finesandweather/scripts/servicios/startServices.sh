#!/bin/bash

echo "Arrancando servicios"

echo "		1)-> Zookeeper start"

cd /home/bigdata/kafka/
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
sleep 2

echo "		2)-> Kafka start"

bin/kafka-server-start.sh -daemon config/server.properties

echo "		3)-> Arrancamos Druid"
echo "		  	->Coordinator start"
cd /home/bigdata/druid-0.12.0/
bin/coordinator.sh start
echo "		  	->Historical start"
bin/historical.sh start
echo "		  	->Broker start"
bin/broker.sh start
echo "		  	->Overlord start"
bin/overlord.sh start
echo "		  	->MiddleManager start"
bin/middleManager.sh start

echo "		4)-> Arrancando grafana"

service grafana-server start

echo "		5)-> Arrancando redis"

cd /home/bigdata/redis-stable/
src/redis-server redis.conf







