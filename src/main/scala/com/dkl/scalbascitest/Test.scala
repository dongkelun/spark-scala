package com.dkl.scalbascitest

import org.apache.spark.sql.SparkSession
import org.dmg.pmml.False
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.io.Text

object Test {

  def main(args: Array[String]): Unit = {

    val path = args(0)
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    //    val rdd = sc.textFile("files/learning/test.txt", 1)
    val rdd = transfer(sc, path, 17)

    var beiginTime = System.currentTimeMillis

    println("=====================================")
    val a = Array("普通车", "C", "a")
    rdd.map(_.split(" +")).take(5).foreach(k => println(k.mkString(",")))
    //    val res = rdd.map(_.split(" +")).filter(arr => {
    //      var flag = false
    //      for (i <- arr if !flag) {
    //        if (a.contains(i))
    //          flag = true
    //      }
    //      flag
    //    })
    //    println(res.count)
    val res1 = rdd.filter(_.split(" +").exists(a.contains(_)))
    println(res1.count)
    var endTime = System.currentTimeMillis
    println("cost " + (endTime - beiginTime) + " milliseconds.")
    //    res.foreach(p => println(p.mkString(",")))

    //
    //     beiginTime = System.currentTimeMillis
    //    println(res1.count)
    //    endTime = System.currentTimeMillis
    //    println("cost " + (endTime - beiginTime) + " milliseconds.")
    spark.stop
    spark.close
  }

  def testOption {
    println(banded(0.555))
    println(banded(2))

  }

  def banded(input: Double) = {
    if (input > 1.0 || input < 0.0)
      None
    else
      Right(math.round(input * 100.0))

  }

  /**
   *
   * 测试cache的效果
   */
  def testCache() {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.textFile("files/test.txt").cache
    var beiginTime = System.currentTimeMillis
    println(rdd.count)
    var endTime = System.currentTimeMillis
    println("cost " + (endTime - beiginTime) + " milliseconds.")

    beiginTime = System.currentTimeMillis
    println(rdd.count)
    endTime = System.currentTimeMillis
    println("cost " + (endTime - beiginTime) + " milliseconds.")
    spark.stop
  }

  def transfer(sc: SparkContext, path: String, numPartitions: Int = 1): RDD[String] = {
    sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], numPartitions)
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
  }
}