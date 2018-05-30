package com.dkl.leanring.spark.sql

import org.apache.spark.sql.SparkSession

object Txt2Df {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Txt2Df").master("local").getOrCreate()
    val data_path = "files/data.csv"
    val df = spark.read.option("header", "true").csv(data_path)
    df.show()
  }
}