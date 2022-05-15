package com.daily.groupExp

import com.daily.JDBCUtils.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.ResultSet
import scala.collection.mutable.ListBuffer

object BlackList {
    private val jDBCUtil = new JDBCUtil
    
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("yarn").setAppName("blackList").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        val ssc = new StreamingContext(sparkConf, Seconds(10))
        ssc.checkpoint("cp2")
        
        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "10.10.43.145:9092,10.10.43.65:9092,10.10.43.20:9092",
            // ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.224.102:9092,192.168.224.103:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "daily",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )
    
        val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("blackListKafka"), kafkaPara)
        )
        // kafkaDataDS.print()
        
        // kafkaDataDS1.print()
        // kafkaDataDS1.foreachRDD(
        //     rdd => {
        //         rdd.collect().foreach(println)
        //     }
        // )

        val ds = kafkaDataDS.transform(
            rdd => {
                val blackList = ListBuffer[String]()

                val conn = jDBCUtil.getConnection
                val pstat = conn.prepareStatement("select user_id from black_list")

                val rs: ResultSet = pstat.executeQuery()
                while (rs.next()) {
                    blackList.append(rs.getString(1))
                }
                rs.close()
                pstat.close()
                conn.close()

                rdd.filter(
                    data => {
                        !blackList.contains(data.toString)
                    }
                )
            }
        )
        // println("来到下一步！")
        val ds1 = ds.map(
            data => (data.value().toInt, 1)
        ).reduceByKeyAndWindow(
            (x: Int, y: Int) => {x + y},
            (x: Int, y: Int) => {x - y},
            Seconds(60), Seconds(20)
        )

        ds1.foreachRDD(
            rdd => {
                // println("此时点击次数为：")
                // rdd.collect().foreach(println)
                val tmp = rdd.filter(
                    data => {
                        data._2 > 30
                    }
                )
                // println("此时黑名单用户为：")
                // tmp.collect().foreach(println)
                tmp.foreach(
                    data => {
                        // println("插入黑名单表的用户id为" + data._1);
                        val conn = jDBCUtil.getConnection
                        val sql = """
                                    |insert into black_list (user_id) values (?)
                                    |on DUPLICATE KEY
                                    |UPDATE content = ?
                                      """.stripMargin
                        val userId = data._1
                        jDBCUtil.executeUpdate(conn, sql, Array( userId, "" ))
                        conn.close()
                    }
                )
            }
        )
    
        ssc.start()
        ssc.awaitTermination()
    }
}
