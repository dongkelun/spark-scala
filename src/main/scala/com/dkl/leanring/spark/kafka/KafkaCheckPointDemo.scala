package com.dkl.leanring.spark.kafka

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object KafkaCheckPointDemo {
  def createContext(checkpointDirectory: String): StreamingContext = {

    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context")
    val sparkConf = new SparkConf().setAppName("KafkaCheckPointDemo").setMaster("local[2]")
    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(checkpointDirectory)

    val server = "10.180.29.180:6667"
    val server1 = "192.168.44.129:9092"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> server, //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "KafkaCheckPointDemo", //消费者组名
      "auto.offset.reset" -> "latest", //latest自动重置偏移量为最新的偏移量earliest
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
        f.foreach(f => {
          val value = f.value()
          println(value)

        })
      }
    })

    ssc
  }

  def main(args: Array[String]) {
    val ip = "10.180.29.180"
    val port = 9999
    val checkpointDirectory = "hdfs://ambari.master.com:8020/spark/dkl/kafka/checkpointdemo"

    //    val sparkConf = new SparkConf().setAppName("RecoverableNetworkWordCount").setMaster("local[2]")
    //    // Create the context with a 1 second batch size
    //    val ssc = new StreamingContext(sparkConf, Seconds(1))
    //    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
    //      () => createContext(checkpointDirectory))

    val sparkConf = new SparkConf().setAppName("KafkaCheckPointDemo").setMaster("local[2]")
    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    //    ssc.checkpoint(checkpointDirectory)
    val server = "10.180.29.180:6667"
    val server1 = "192.168.44.129:9092"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> server, //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "KafkaCheckPointDemo", //消费者组名
      "auto.offset.reset" -> "earliest", //latest自动重置偏移量为最新的偏移量earliest
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
        f.foreach(f => {
          val value = f.value()
          println(value)

        })
        val offsetRanges = f.asInstanceOf[HasOffsetRanges].offsetRanges
        f.foreachPartition { iter =>
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }

        //         some time later, after outputs have completed
        //        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

      }
    })
    //    stream.foreachRDD { rdd =>
    //      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //      rdd.foreachPartition { iter =>
    //        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
    //        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
    //      }
    //    }
    ssc.start()
    ssc.awaitTermination()
  }
}