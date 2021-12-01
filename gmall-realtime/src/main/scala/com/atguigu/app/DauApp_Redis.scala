package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 作用：
  * 1）建立与kafka的连接，从kafka中读取数据；
  * 2）进行批次间和批次内去重，并存入redis中；
  * 3）明细数据存入Hbase
  */

object DauApp_Redis {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //3.获取kafka的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    //4.将JSON格式的数据转为样例类，并补全logDate & logHour两个时间字段
    /**
      * 思考逻辑：1）确定是否需要返回值；2）实现的功能--时间转换；3)确定使用的算子；
      * 注：mapPartitions是sparkstreaming中的原语,map是scala中的函数
      */
    //SimpleDateFormat底层实现了序列化接口，故创建在Driver端在Executor端不会报错
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //将数据转为样例类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        val times: String = sdf.format(new Date(startUpLog.ts))
        //补全logDate
        startUpLog.logDate = times.split(" ")(0)
        //补全logHour
        startUpLog.logHour = times.split(" ")(1)
        //返回样例类
        startUpLog
      })
    })

    //优化：缓存多次使用的流----序列化缓存级别-MEMORY_ONLY_SER
    startUpLogDStream.cache()

    //5. 进行批次间去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream)

    //优化：缓存多次使用的流----序列化缓存级别-MEMORY_ONLY_SER
    filterByRedisDStream.cache()

    //打印原始数据的个数
    startUpLogDStream.count().print()
    //打印批次间去重后的个数
    filterByRedisDStream.count().print()


    //6. 进行批次内去重


    //7. 将去重后的（mid）保存到redis
    /**
      * 思考逻辑：
      * 1）存什么--mid；
      * 2）用什么类型--set
      *   数据格式为k(kv,kv,kv)且需要去重
      *   string--单个kv，不适合
      *   list--不能去重，不适合
      *   set--可去重并且满足k(kv)格式
      * hash--kv中仍为kv，不适合
      * 3）redisKey怎么设计--"Dau:"+logDate
      */
    DauHandler.saveToRedis(filterByRedisDStream)



    //8. 将去重后的明细数据保存到Hbase


    //先启动，堵塞当前线程---如果启动前阻塞会造成无法启动
    ssc.start()
    ssc.awaitTermination()
  }
}
