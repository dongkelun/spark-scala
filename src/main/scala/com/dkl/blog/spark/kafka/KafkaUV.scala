package com.dkl.blog.spark.kafka

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

/**
 * 博客：SparkStreaming+Kafka 实现统计基于缓存的实时uv
 * https://dongkelun.com/2018/06/25/KafkaUV/
 */
object KafkaUV {
  def main(args: Array[String]): Unit = {
    //初始化，创建SparkSession
    val spark = SparkSession.builder().appName("KafkaUV").master("local[2]").enableHiveSupport().getOrCreate()
    //初始化，创建sparkContext
    val sc = spark.sparkContext
    //初始化，创建StreamingContext，batchDuration为5秒
    val ssc = new StreamingContext(sc, Seconds(5))

    //开启checkpoint机制
    ssc.checkpoint("hdfs://ambari.master.com:8020/spark/dkl/kafka/UV_checkpoint")

    //kafka集群地址
    val server = "ambari.master.com:6667"

    //配置消费者
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> server, //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "UpdateStateBykeyWordCount", //消费者组名
      "auto.offset.reset" -> "latest", //latest自动重置偏移量为最新的偏移量   earliest 、none
      "enable.auto.commit" -> (false: java.lang.Boolean)) //如果是true，则这个消费者的偏移量会在后台自动提交
    val topics = Array("KafkaUV") //消费主题

    //基于Direct方式创建DStream
    val stream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    //开始执行WordCount程序

    //以空格为切分符切分单词，并转化为 (word,1)形式
    val words = stream.flatMap(_.value().split(" ")).map((_, 1))
    val wordCounts = words.updateStateByKey(
      //每个单词每次batch计算的时候都会调用这个函数
      //第一个参数为每个key对应的新的值，可能有多个，比如(hello,1)(hello,1),那么values为(1,1)
      //第二个参数为这个key对应的之前的状态
      (values: Seq[Int], state: Option[Int]) => {

        var newValue = state.getOrElse(0)
        values.foreach(newValue += _)
        Option(newValue)

      })

    //共享变量，便于后面的比较是否用新的uv
    val accum = sc.longAccumulator("uv")

    wordCounts.foreachRDD(rdd => {

      //如果uv增加
      if (rdd.count > accum.value) {
        //打印uv
        println(rdd.count)
        //将共享变量的值更新为新的uv
        accum.add(rdd.count - accum.value)
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
