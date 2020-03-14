package com.dkl.blog.spark.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
  * Created by dongkelun on 2020/1/21 14:20
  *
  * Spark DataFrame isin方法使用
  */
object SparkDfIsIn {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkDfIsIn")
      .master("local")
      .getOrCreate()

    val data = Array(("001", "张三", 21, "2018"), ("002", "李四", 18, "2017"),
      ("003", "sam", 18, "2019"), ("004", "abby", 23, "20117")
    )

    val df = spark.createDataFrame(data).toDF("id", "name", "age", "year")
    df.show()

    val yearArray = Array("2017", "2018")
    df.where(col("year").isin(yearArray: _*)).show()

    val data1 = Array(("001", "张三", 21, "2018"), ("002", "李四", 18, "2017"))
    val df1 = spark.createDataFrame(data1).toDF("id", "name", "age", "year")
    //DataFame 需要先将该列查出来去重转出数组，
    val df_yearArray = df1.select("year").distinct().collect().map(_.getAs[String]("year"))

    df.where(col("year").isin(df_yearArray: _*)).show()

    spark.close()
  }
}
