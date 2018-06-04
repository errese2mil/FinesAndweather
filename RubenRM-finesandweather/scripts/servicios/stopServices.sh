#!/bin/bash

echo "Parando servicios"

echo "		1)-> Kafka stop"

cd /home/bigdata/kafka/
bin/kafka-server-stop.sh

sleep 5
echo "		2)-> Druid stop"

cd /home/bigdata/druid-0.12.0/
echo "		  	->MiddleManager stop"
bin/middleManager.sh stop
echo "		  	->Overlord stop"
bin/overlord.sh stop
echo "		  	->Broker stop"
bin/broker.sh stop
echo "		  	->Historical stop"
bin/historical.sh stop
echo "		  	->Coordinator stop"
bin/coordinator.sh stop

sleep 5
echo "		3)-> Zookeeper stop"

cd /home/bigdata/kafka/
bin/zookeeper-server-stop.sh

sleep 5
echo "		4)-> Grafana stop"

service grafana-server stop

sleep 5
echo "		5)-> Redis stop"

cd /home/bigdata/redis-stable/
src/redis-cli shutdown





