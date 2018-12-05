package com.dkl.leanring.spark.hdfs

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.compress.BZip2Codec

object SaveBzip2File {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SaveBzip2File").master("local").getOrCreate()
    val sc = spark.sparkContext

    var data = Array(1, 2, 3)

    val rdd = sc.parallelize(data, 1)

    rdd.saveAsTextFile("codec/bzip2", classOf[BZip2Codec])

  }
}
