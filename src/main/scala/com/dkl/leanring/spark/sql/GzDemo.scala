package com.dkl.leanring.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.io.Text

object GzDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("CsvDemo").master("local").getOrCreate()

    val sc = spark.sparkContext
    val path = "files/数据统计20180416.tar.gz"
    val path1 = "D:/我的文档/sdhs/数据统计20180416/数据统计20180416.txt"
    val path2 = "D:/我的文档/sdhs/数据统计20180416.rar"
    val path3 = "hdfs://ambari.master.com:8020/tmp/dkl/data.tar.gz"
    val path4 = "hdfs://ambari.master.com:8020/tmp/dkl/t1.txt"
    val path5 = "hdfs://ambari.master.com:8020/tmp/dkl/数据统计20180416.tar.gz"
    val starttime = System.nanoTime
    val rdd = sc.textFile(path3)
    //    println(rdd.count())
    rdd.take(6).foreach(println)
    transfer(sc, path5).take(6).foreach(println)
    val endtime = System.nanoTime
    val delta = endtime - starttime
    spark.stop
    println("所用时间：" + delta / 1000000 + "ms")
  }

  def transfer(sc: SparkContext, path: String): RDD[String] = {
    sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1)
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
  }
}