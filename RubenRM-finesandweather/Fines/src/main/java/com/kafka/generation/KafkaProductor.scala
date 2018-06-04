package com.kafka.generation

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


case class KafkaProductor(kafkaServer: String, topic: String){

  val props = new Properties()
  props.put("bootstrap.servers", kafkaServer)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String,String](props)

  def send(line: String): Unit = {
    producer.send(new ProducerRecord[String,String](topic,"",line))
  }

  def close():Unit={
    producer.close()
  }
}
