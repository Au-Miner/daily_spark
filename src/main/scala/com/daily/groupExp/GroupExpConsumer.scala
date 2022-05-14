package com.daily.groupExp

import com.daily.kafkaUtils.GroupProducer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.api.java.JavaPairDStream.fromPairDStream
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object GroupExpConsumer {
    private val groupProducer = new GroupProducer
    
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("groupExp").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        val ssc = new StreamingContext(sparkConf, Seconds(10))
        ssc.checkpoint("cp1")

        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.224.102:9092,192.168.224.103:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "daily",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )
        
        val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("userDoGroupJobKafka"), kafkaPara)
        )
        // kafkaDataDS.print()
        val kafkaDataDS1 = kafkaDataDS.map(
            kafkaData => {
                val data = kafkaData.value()
                val datas = data.split(" ")
                if (datas(1) == "1") {
                    GroupExpData(datas(0).toInt, 1, 1)
                }
                else {
                    GroupExpData(datas(0).toInt, -1, 0)
                }
            }
        )
        solve2(kafkaDataDS1)
        
        ssc.start()
        ssc.awaitTermination()
    }
    
    def solve2(DS: DStream[GroupExpData]): Unit = {
        val DS1 = DS.transform(
            rdd => {
                rdd.filter(
                    data => {
                        data.groupRecExp > 0
                    }
                )
            }
        )
        val DS2 = DS1.map(
            data => {
                (data.groupId, data.groupRecExp)
            }
        ).reduceByKeyAndWindow(
            (x: Int, y: Int) => {x + y},
            (x: Int, y: Int) => {x - y},
            Seconds(86400), Seconds(20)
        )
        // DS2.print()
        DS2.foreachRDD(
            rdd => {
                val tuples = rdd.sortBy(_._2, false).take(10)
                val ints = tuples.map(_._1)
                // ints.foreach(print)
                // println()
                groupProducer.send(ints);
            }
        )
    }
    
    case class GroupExpData( groupId: Int, groupAllExp: Int, groupRecExp: Int )
}
