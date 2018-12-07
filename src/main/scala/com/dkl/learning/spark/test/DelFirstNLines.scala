package com.dkl.learning.spark.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors

object DelFirstNLines {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DelFirstNLines").master("local").getOrCreate()
    //    import spark.implicits._
    val data = Array((Vectors.dense(Array(0.0))), (Vectors.dense(Array(2.0))))

    //    val df = spark.createDataFrame(data, Array("a"))

    val sc = spark.sparkContext

    val path = "files/data.txt"
    //val path = "hdfs://ambari.master.com:8020/tmp/dkl/data.txt"
    //    val path = "hdfs://ambari.master.com:8020/tmp/dkl/t1.txt"
    val rdd = sc.textFile(path, 8)
    rdd.repartition(3).take(3).foreach(println)
    println("***********")
    rdd.coalesce(3).take(3).foreach(println)
    println("***********")
    //    val rdd3 = rdd.repartition(4)
    println("分区数：" + rdd.getNumPartitions)
    val arr = rdd.take(3)
    val first = rdd.first()
    val rdd3 = rdd.filter(!arr.contains(_))
    rdd3.foreach(println)
    //    return
    //    spark.stop()
    println("分区数：" + rdd.getNumPartitions)
    val rdd1 = rdd.zipWithIndex()
    //过滤掉索引小于等于2的
    val rdd2 = rdd1.filter(_._2 > 2)
    rdd1.foreach(println)
    println("**********分割线***********")
    rdd2.map(kv => kv._1).foreach(println)
    spark.stop()
  }
}
