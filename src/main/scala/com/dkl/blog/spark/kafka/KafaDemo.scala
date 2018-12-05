package com.dkl.blog.spark.kafka

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * 博客：Spark Streaming连接Kafka入门教程
 * https://dongkelun.com/2018/05/17/sparkKafka/
 */
object KafaDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafaDemo")
    //刷新时间设置为1秒
    val ssc = new StreamingContext(conf, Seconds(1))
    //消费者配置
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.180.29.180:6667", //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group", //消费者组名
      "auto.offset.reset" -> "latest", //latest自动重置偏移量为最新的偏移量
      "enable.auto.commit" -> (false: java.lang.Boolean)) //如果是true，则这个消费者的偏移量会在后台自动提交
    val topics = Array("top1") //消费主题，可以同时消费多个
    //创建DStream，返回接收到的输入数据
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    //打印获取到的数据，因为1秒刷新一次，所以数据长度大于0时才打印
    stream.foreachRDD(f => {
      if (f.count > 0)
        f.foreach(f => println(f.value()))
    })
    ssc.start();
    ssc.awaitTermination();
  }
}
