package com.guoyao

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
读取本地文件一行行发送kafka topic模拟生产环境
 */
object KafkaProducerUtil {
  def main(args: Array[String]): Unit = {
    writeToKafka("testTopic")
  }
  def writeToKafka(topic: String): Unit ={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.0.240:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)

    // 从文件读取数据，逐行写入kafka
    val bufferedSource = io.Source.fromFile("E:\\370797000.txt")

    for( line <- bufferedSource.getLines() ){
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
      print(record+"\n")
      Thread.sleep(100)
    }

    producer.close()
  }
}
