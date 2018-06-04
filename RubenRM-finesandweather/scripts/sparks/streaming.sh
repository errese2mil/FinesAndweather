#!/bin/bash

#export HADOOP_CONF_DIR=/opt/software/hadoop-2.7.4/etc/hadoop

spark-submit --verbose --class com.kafka.consumer.Consumer --master local[*] --driver-memory 4G --executor-memory 4G /home/bigdata/IdeaProjects/FinesAndWeather/StreamingConsumer/target/StreamingConsumer-1.0-SNAPSHOT.jar


