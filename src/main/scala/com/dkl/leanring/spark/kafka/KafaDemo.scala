package com.dkl.leanring.spark.kafka

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.antlr.stringtemplate.language.ConditionalExpr.ElseIfClauseData

object KafaDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("KafaDemo")
    //刷新时间设置为1秒
    val ssc = new StreamingContext(conf, Seconds(1))
    //消费者配置
    val server = "10.180.29.180:6667"
    val server1 = "192.168.44.129:9092"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> server, //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group", //消费者组名
      "auto.offset.reset" -> "latest", //latest自动重置偏移量为最新的偏移量earliest
      "enable.auto.commit" -> (false: java.lang.Boolean)) //如果是true，则这个消费者的偏移量会在后台自动提交
    val topics = Array("top1") //消费主题
    //创建DStream，返回接收到的输入数据
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    //打印获取到的数据，因为1秒刷新一次，所以数据长度大于0时才打印
    // 说明，传过来的数据字段顺序要一致
    val colName = Array("ip", "uid", "其他字段，自己添加", "firstbuffer")
    stream.foreachRDD(f => {

      if (f.count > 0) {
        f.foreach(f => {
          val value = f.value()

          val arr = value.split("\\|")
          if (arr(3).toInt == -2)
            println("需要存到数据")
          else
            println("不需要存到数据，只是打印出来")
          if (colName.length == arr.length) {

            for (i <- 0 until colName.length) {
              print(colName(i) + ":" + arr(i) + " ")

            }
            println()
          }

        })
      }
    })
    ssc.start();
    ssc.awaitTermination();
  }
}