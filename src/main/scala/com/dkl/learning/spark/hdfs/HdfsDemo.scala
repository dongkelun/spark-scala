package com.dkl.learning.spark.hdfs

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.mapred.KeyValueTextInputFormat
import org.apache.spark.rdd.HadoopRDD
import org.apache.hadoop.mapred.FileSplit

object HdfsDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    val path = "hdfs://ambari.master.com:8020/tmp/dkl"

    //    val text = sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1)
    //    //    val input = sc.hadoopFile[Text, Text, KeyValueTextInputFormat](path).map {
    //    //      case (x, y) => (x.toString, y.toString)
    //    //    }
    //    //    input.foreach(println)
    //    val hadoopRdd = text.asInstanceOf[HadoopRDD[LongWritable, Text]]
    //
    //    val fileAndLine = hadoopRdd.mapPartitionsWithInputSplit { (inputSplit, iterator) ⇒
    //      val file = inputSplit.asInstanceOf[FileSplit]
    //
    //      iterator.map { tpl ⇒ (file.getPath, file.getLocationInfo, file.getLocations.mkString, tpl._2) }
    //    }
    //
    //    fileAndLine.foreach(println)
    //    println(hadoopRdd.count)
    //    println(hadoopRdd.collect().mkString(" "))

    val filesRdd = sc.wholeTextFiles(path)
    val fileNameRdd = filesRdd.map(fileNameContent => {
      val fileName = fileNameContent._1
      fileName
    })
    fileNameRdd.take(3).foreach(println)
    spark.stop()
    spark.close()
  }

  def transfer(sc: SparkContext, path: String): RDD[String] = {
    sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1)
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
  }
}
