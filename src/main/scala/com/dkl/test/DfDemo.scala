package com.dkl.test

import org.apache.spark.sql.SparkSession

object DfDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()

    val data_path = "files/data.csv"
    val df = spark.read.option("header", "true").csv(data_path)
    df.show()
    
  }
}