package com.dkl.learning.spark.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds

object CheckPointDemo {
  def createContext(checkpointDirectory: String): StreamingContext = {

    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context")
    val sparkConf = new SparkConf().setAppName("KafkaCheckPointDemo").setMaster("local[2]")
    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(checkpointDirectory)

    ssc
  }
  def main(args: Array[String]): Unit = {
    val checkpointDirectory = "hdfs://ambari.master.com:8020/spark/dkl/kafka/checkpointdemo1"
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(checkpointDirectory))
  }

}
