package com.dkl.leanring.spark.exception

import org.apache.spark.sql.SparkSession

/**
 * spark-submit报错:Application application_1529650293575_0148 finished with failed status
 *
 * 博客:https://dongkelun.com/2018/07/06/sparkSubmitException1/
 */
object YarnClusterDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("YarnClusterDemo").master("local").getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.parallelize(Seq(1, 2, 3))
    println(rdd.count)

    spark.stop()
  }
}
