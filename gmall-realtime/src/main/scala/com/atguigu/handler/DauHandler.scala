package com.atguigu.handler

import java.lang

import com.atguigu.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {

  /**
    * 进行批次间去重
    * @param startUpLogDStream
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog]) = {

    /*//方案一
      val value: DStream[StartUpLog] = startUpLogDStream.filter(startUplog => {
      //1.获取redis连接
      val jedis = new Jedis("hadoop105", 6379)

      //定义key的格式
      val redisKey: String = "DAU:" + startUplog.logDate
      //2. 判断mid在redis中有没有--利用redis中的方法sismember
      val boolean: lang.Boolean = jedis.sismember(redisKey, startUplog.mid)

      //关闭资源
      jedis.close()
      //当boolean为true时，则在redis中存在，需要执行过滤
      //filter算子--当满足条件时保留，反之过滤，所以当boolean为true时，filter应该为false；
      !boolean
    })
    value*/

    //方案二：在每个分区下获取连接，以减少连接个数
    //mapPartitions有返回值，foreachPartitions没有返回值
    val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      //在分区下创建redis连接
      val jedis = new Jedis("hadoop105", 6379)
      val logs: Iterator[StartUpLog] = partition.filter(startUplog => {
        //定义key的格式
        val redisKey: String = "DAU:" + startUplog.logDate
        //2. 判断mid在redis中有没有--利用redis中的方法sismember
        val boolean: lang.Boolean = jedis.sismember(redisKey, startUplog.mid)

        !boolean
      })
      jedis.close()
      logs
    })
    value
  }

  /**
    * --将去重后的（mid）保存到redis
    * * 写库操作一般使用foreachRDD
    * @param startUpLogDStream
    */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {

    startUpLogDStream.foreachRDD(rdd=> {
      //**此位置执行在Driver端，如果在该位置获取连接在Executor端使用，会序列化报错
      rdd.foreachPartition(partition=>{
        //**此位置执行每个分区获取一次连接
        //创建redis连接
        val jedis = new Jedis("hadoop105",6379)

        partition.foreach(log=>{
          //**此位置执行每条数据获取一次连接
          //确定存入redis中的类型和kv----sadd确定使用类型为set，key的格式为DAU+logDate
          val redisKey: String = "DAU:"+log.logDate
          jedis.sadd(redisKey,log.mid)

        })
        jedis.close()
      })
    })
  }
}
