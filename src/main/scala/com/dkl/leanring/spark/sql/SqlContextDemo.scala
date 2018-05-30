package com.dkl.leanring.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SqlContextDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SqlContextDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val data = Array((3, "name3"), (4, "name4"), (5, "name5"))
    val df = sc.parallelize(data).toDF("id", "name")
    df.createOrReplaceTempView("user")
    val df1 = sc.parallelize(data).toDF("id", "name")
    df1.createOrReplaceTempView("user1")
    import sqlContext.sql
    sql("select * from user").show(2)
    sqlContext.uncacheTable("user1")
    sql("select * from user").show(3)
  }
}