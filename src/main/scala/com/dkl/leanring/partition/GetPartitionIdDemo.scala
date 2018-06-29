package com.dkl.leanring.partition

import org.apache.spark.sql.SparkSession
import org.apache.spark.TaskContext

/**
 * 获取当前分区的partitionId
 */
object GetPartitionIdDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GetPartitionIdDemo").master("local").getOrCreate()
    val sc = spark.sparkContext
    val data = Seq(1, 2, 3, 4)

    // 测试rdd,三个分区
    val rdd = sc.parallelize(data, 3)
    rdd.foreach(i => {
      println("partitionId：" + TaskContext.get.partitionId)
    })

    import spark.implicits._
    // 测试df,三个分区
    val df = rdd.toDF("id")
    df.show
    df.foreach(row => {
      println("partitionId：" + TaskContext.get.partitionId)
    })
    // 测试df,两个分区
    val data1 = Array((1, 2), (3, 4))
    val df1 = spark.createDataFrame(data1).repartition(2)
    df1.show()
    df1.foreach(row => {
      println("partitionId：" + TaskContext.get.partitionId)
    })

  }
}