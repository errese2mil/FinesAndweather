#!/bin/bash

#export HADOOP_CONF_DIR=/opt/software/hadoop-2.7.4/etc/hadoop

spark-submit --verbose --class com.kafka.generation.Productor --master local[*] --driver-memory 1G --executor-memory 1G /home/bigdata/IdeaProjects/FinesAndWeather/Weather/target/Weather-1.0-SNAPSHOT.jar


