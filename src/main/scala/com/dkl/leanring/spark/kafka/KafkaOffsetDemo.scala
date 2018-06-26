package com.dkl.leanring.spark.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.TaskContext
import org.apache.kafka.common.TopicPartition

object KafkaOffsetDemo {
  def main(args: Array[String]) {

    //创建sparkConf
    val sparkConf = new SparkConf().setAppName("KafkaOffsetDemo").setMaster("local[2]")
    // 创建StreamingContext batch sizi 为 1秒
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ambari.master.com:6667", //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "KafkaOffsetDemo", //消费者组名
      "auto.offset.reset" -> "earliest", //当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      "enable.auto.commit" -> (false: java.lang.Boolean)) //如果是true，则这个消费者的偏移量会在后台自动提交
    val topics = Array("top1") //消费主题
    //创建DStream，返回接收到的输入数据
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    // 打印获取到的数据，因为1秒刷新一次，所以数据长度大于0时才打印
    stream.foreachRDD(f => {

      if (f.count > 0) {
        println("=============================")
        println("打印获取到的kafka里的内容")
        f.foreach(f => {
          val value = f.value()
          println(value)

        })
        println("=============================")
        println("打印offset的信息")
        // offset
        val offsetRanges = f.asInstanceOf[HasOffsetRanges].offsetRanges

        //打印offset
        f.foreachPartition { iter =>
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }
        println("=============================")
        // 等输出操作完成后提交offset
        //        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

      }
    })
    //启动
    ssc.start()
    //等待停止
    ssc.awaitTermination()
  }
}