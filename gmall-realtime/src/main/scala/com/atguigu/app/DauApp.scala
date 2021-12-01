package com.atguigu.app

import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 作用：建立与kafka的连接，从kafka中读取数据；
  */

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //3.获取kafka的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    //4.打印测试
    /**
      * 使用mapPartitions和直接用map的区别
      * 1. 直接使用map是将所有分区的数据放在一个线程中，执行效率低；
      * 2. 使用mapPartitions则会先遍历分区，再遍历分区中的数据，是多线程并行执行，故效率高；
      * 3. 但是mapPartitions是先将数据放在一个partition迭代器（集合）中，集合中的数据存储在内存中，
      * 所以当某个分区的数据量过大时，会造成OOM内存溢出；
      */
    val value: DStream[String] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        record.value()
      })
    })
    value.print()


    //先启动，堵塞当前线程---如果启动前阻塞会造成无法启动
    ssc.start()
    ssc.awaitTermination()
  }
}
