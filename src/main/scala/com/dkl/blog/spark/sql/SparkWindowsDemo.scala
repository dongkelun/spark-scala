package com.dkl.blog.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
/**
  * Created by dongkelun on 2020/6/22 21:39
  *
  * spark 窗口函数示例
  */
object SparkWindowsDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df = List("1 2018-02-03 2019-02-03", "2 2019-02-04 2020-03-04", "3 2018-08-04 2019-03-04").toDF()

    df.show()
    df.as[String]
      .map(str => str.split(" ")(1) + " " + str.split(" ")(2))
      .flatMap(str => str.split("\\s")).show()

    val w1 = Window.orderBy($"value" desc).rowsBetween(0,2)
    df.as[String]
      .map(str => str.split(" ")(1) + " " + str.split(" ")(2))
      .flatMap(str => str.split("\\s"))
      .distinct()
//      .sort($"value" asc)
      .withColumn("new", max("value") over (w1))
      .withColumn("new1", min("value") over (w1))
      .show()

  }
}
