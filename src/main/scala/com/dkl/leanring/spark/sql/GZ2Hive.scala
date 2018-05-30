package com.dkl.leanring.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SaveMode

object GZ2Hive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DelFirstNLines").enableHiveSupport().getOrCreate()

    val sc = spark.sparkContext
    //    val path = "files/data.txt"
    //    val path = "hdfs://ambari.master.com:8020/tmp/dkl/data.txt"
    val path = "hdfs://ambari.master.com:8020/tmp/dkl/t1.txt"
    //    val data = sc.textFile(path)
    val data = transfer(sc, path)
    data.cache()
    val arr = data.take(3)
    val rdd = data.filter(!arr.contains(_))

    //第二行为列名
    val colName = arr(1).split(" +") // +表示根据一个或多个空格进行分割
    val schema = StructType(colName.map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = rdd.map(_.split(" +")).filter(_.length == 9).map(p => Row(p: _*))

    val tmp = rdd.filter(_.split(" +").length != 9)
    println(tmp.first())
    println(rowRDD.count)
    val df = spark.createDataFrame(rowRDD, schema)
    df.show(false)
    spark.sql("use dkl")
    df.persist()
    df.write.mode(SaveMode.Overwrite).saveAsTable("t1")
    spark.stop()
  }

  def transfer(sc: SparkContext, path: String): RDD[String] = {
    sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1)
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
  }
}