package com.daily.kafkaUtils

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.collection.mutable.ArrayBuffer

class GroupProducer {
    def send(msg: Array[Int]): Unit = {
        val properties = new Properties()
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.224.102:9092,192.168.224.103:9092")
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        
        var producer = new KafkaProducer[String, String](properties)
    
        var res = "";
        for (tmp <- msg) {
            res += tmp.toString + " "
        }
        producer.send(new ProducerRecord[String, String]("second", res))
        
        
        producer.close()
    }
}
