package com.dkl.leanring.spark.sql

import org.apache.spark.sql.SparkSession

object WriteDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("WriteDemo")
      .master("local")
      .getOrCreate()
    val data = Array((1, "val1"), (2, "val2"), (3, "val3"))
    var df = spark.createDataFrame(data).toDF("key", "value")
    df.write.mode("overwrite").format("csv").save("test.csv")
  }
}