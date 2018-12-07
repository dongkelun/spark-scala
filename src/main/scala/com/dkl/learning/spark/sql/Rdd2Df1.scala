package com.dkl.learning.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Rdd2Df1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Rdd2Df").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data_path = "files/data2.txt"
    val data = sc.textFile(data_path)
    val first = data.first //第一行作为列名
    val colName = first.split(",")
    val rdd = data.filter(_ != first) //注意first是列名，在这里的txt里是唯一的，否则会过滤掉多行
    val df = rdd.map(_.split(",")).map(p => (p(0), p(1), p(2), p(3), p(4))).toDF(colName: _*)
    df.show
  }
}
