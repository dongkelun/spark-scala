package com.dkl.learning.spark.ml
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
object NetworkWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("10.180.29.180", 9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start() // Start the computation
    ssc.awaitTermination()
  }
}
