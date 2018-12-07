package com.dkl.learning.spark.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.kafka010.CanCommitOffsets
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.util.Properties
import java.util.Arrays

object ResetKafkaOffset {

  def seek(args: Array[String]) = {
    val topic = args(2);
    val partition = Integer.parseInt(args(3));
    val offset = Integer.parseInt(args(4));

    val props = new Properties();
    props.put("bootstrap.servers", "ambari.master.com:6667");
    props.put("group.id", "spark-executor-KafkaOffsetDemo");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    val consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);
    //consumer.subscribe(Arrays.asList(topic)); //"deviceInfoTopic"
    val topicPartition = new TopicPartition(topic, partition);
    consumer.assign(Arrays.asList(topicPartition));

    consumer.seek(new TopicPartition(topic, partition), offset);
    consumer.close();
    "SUCC";
  }

  def main(args: Array[String]) {

    if ("seek".equals(args(1))) {
      System.out.println(seek(args));
    }
    //    //创建sparkConf
    //    val sparkConf = new SparkConf().setAppName("ResetKafkaOffset").setMaster("local[2]")
    //    // 创建StreamingContext batch sizi 为 1秒
    //    val ssc = new StreamingContext(sparkConf, Seconds(1))
    //    val kafkaParams = scala.collection.mutable.Map[String, Object](
    //      "bootstrap.servers" -> "10.180.29.180:6667", //kafka集群地址
    //      "key.deserializer" -> classOf[StringDeserializer],
    //      "value.deserializer" -> classOf[StringDeserializer],
    //      "group.id" -> "ResetKafkaOffset", //消费者组名
    //      "auto.offset.reset" -> "earliest", //当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
    //      "enable.auto.commit" -> (false: java.lang.Boolean)) //如果是true，则这个消费者的偏移量会在后台自动提交
    //    val topics = Array("top1") //消费主题
    //
    //    //创建DStream，返回接收到的输入数据
    //
    //    val stream = KafkaUtils.createDirectStream[String, String](
    //      ssc,
    //      PreferConsistent,
    //      Subscribe[String, String](topics, kafkaParams))
    //    kafkaParams("group.id") = "KafkaOffsetDemo"
    //
    //    println(kafkaParams("group.id"))
    //    val stream1 = KafkaUtils.createDirectStream[String, String](
    //      ssc,
    //      PreferConsistent,
    //      Subscribe[String, String](topics, kafkaParams))
    //
    //    stream.foreachRDD(f => {
    //      val offsetRanges = f.asInstanceOf[HasOffsetRanges].offsetRanges
    //      // 等输入操作完成后提交offset
    //      f.foreachPartition { iter =>
    //        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
    //        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
    //      }
    //      stream1.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    //      println("ok")
    //      stream1.asInstanceOf[CanCommitOffsets].commitAsync(Array(OffsetRange("top1", 0, 134, 190)))
    //    })
    //    //    println(stream1.count())
    //    //    stream.print()
    //    //    stream1.asInstanceOf[CanCommitOffsets].commitAsync(Array(OffsetRange("top1", 0, 134, 190)))
    //
    //    ssc.start()
    //    ssc.awaitTermination()
    //    ssc.stop(true)

  }
}
