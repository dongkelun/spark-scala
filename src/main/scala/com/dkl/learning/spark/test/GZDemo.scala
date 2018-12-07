package com.dkl.leanring.spark.test

import org.apache.spark.sql.SparkSession

object GZDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GZDemo").master("local").getOrCreate()
    val sc = spark.sparkContext
    val path = "hdfs://ambari.master.com:8020/tmp/dkl/data.txt.gz"
    val path4 = "hdfs://ambari.master.com:8020/tmp/dkl/data.tar.gz"
    val path3 = "hdfs://ambari.master.com:8020/tmp/dkl/data_gbk.txt"
    val path1 = "files/test/data.txt.gz"
    val path2 = "files/test/data.txt"
    val path5 = "files/test/data.tar.gz"
    val data = sc.textFile(path4)
    data.take(3).foreach(println)
    transfer(sc, path3).take(3).foreach(println)
    import org.apache.spark.rdd.RDD
    import org.apache.spark.SparkContext
    import org.apache.hadoop.io.LongWritable
    import org.apache.hadoop.mapred.TextInputFormat
    import org.apache.hadoop.io.Text
    def transfer(sc: SparkContext, path: String): RDD[String] = {
      sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1)
        .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
    }
  }
}