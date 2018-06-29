package com.dkl.leanring.spark.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.TaskContext

object TestDf {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JdbcDemo").master("local").getOrCreate()
    val data = Array(("1", "2", "3", "4", "5"), ("6", "7", "8", "9", "10"))
    val df = spark.createDataFrame(data).toDF("col1", "col2", "col3", "col4", "col5")
    val data1 = Array(("1", "2"), ("3", "3"), ("1", "2"))
    val df1 = spark.createDataFrame(data1).toDF("col1", "col2")
    df1.show()
    df1.distinct().show()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(data1, 2)
    rdd.foreach(row => {

      val id = TaskContext.get.partitionId
      println(id)

    })
    //   testFilter(df1)
  }

  def testFilter(df: DataFrame) {

    df.filter(df("col1") === df("col2")).show
    df.filter("col1==col2").show
  }
}