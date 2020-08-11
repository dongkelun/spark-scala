package com.dkl.blog.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by dongkelun on 2019/12/2 19:27
  */
object Test_RegExp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("NewUVDemo").master("local").getOrCreate()

    import spark.implicits._
    import spark.sql

    val df = spark.sparkContext.parallelize(
      Array(
        ("001", "张三"), ("002", "张三1"), ("003", "2张三")
      )).toDF("ID","NAME")
    df.createOrReplaceTempView("TEST_REGEXP")

    sql("select * from TEST_REGEXP where name rlike '\\\\d+'").show()
    sql("select * from TEST_REGEXP where name rlike '[0-9]+'").show()
    sql("select * from TEST_REGEXP where name regexp '\\\\d+'").show()
    sql("select * from TEST_REGEXP where name regexp '[0-9]+'").show()

    spark.close()
  }
}
