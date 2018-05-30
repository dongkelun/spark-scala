package com.dkl.leanring.spark.sql

import org.apache.spark.sql.SparkSession

object CsvDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("CsvDemo").master("local").getOrCreate()
    val df = spark.read.option("header", "true").csv("src/main/resources/scala/alipay.csv")
    df.select("CARD_NO", "cert_no").show()
  }
}