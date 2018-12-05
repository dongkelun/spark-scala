package com.dkl.blog.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.annotation.InterfaceStability

/**
 * 博客：旧版Spark（1.6版本） 将RDD动态转为DataFrame
 * https://dongkelun.com/2018/05/11/rdd2df/
 */
object Rdd2Df {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Rdd2Df").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data_path = "files/data2.txt"
    val data = sc.textFile(data_path)
    val first = data.first //第一行作为列名
    val colName = first.split(",")
    val rdd = data.filter(_ != first) //注意first是列名，在这里的txt里是唯一的，否则会过滤掉多行
    //列名
    val schema = StructType(colName.map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = rdd.map(_.split(",")).map(p => Row(p: _*))
    val df = sqlContext.createDataFrame(rowRDD, schema)
    df.show

  }
}
