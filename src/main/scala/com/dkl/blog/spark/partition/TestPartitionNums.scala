package com.dkl.blog.spark.partition

import org.apache.spark.sql.SparkSession
import scala.collection.Seq
import scala.reflect.api.materializeTypeTag

/**
 * 从代码里的内部数据集创建的默认分区数
 * 博客：https://dongkelun.com/2018/08/13/sparkDefaultPartitionNums/
 */
object TestPartitionNums {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestPartitionNums")
      .master("local")
      .config("spark.default.parallelism", 8)
      .getOrCreate()

    val sc = spark.sparkContext

    println("默认的并行度: " + sc.defaultParallelism)

    println("sc.parallelize 默认分区：" + sc.parallelize(1 to 30).getNumPartitions)
    println("sc.parallelize 参数指定，参数大于sc.defaultParallelism时：" + sc.parallelize(1 to 30, 100).getNumPartitions)
    println("sc.parallelize  参数指定，参数小于sc.defaultParallelism时：" + sc.parallelize(1 to 30, 3).getNumPartitions)

    var data = Seq((1, 2), (1, 2), (1, 2), (1, 2), (1, 2))

    println("spark.createDataFrame data的长度小于sc.defaultParallelism时，长度：" + data.length + " 分区数：" + spark.createDataFrame(data).rdd.getNumPartitions)
    data = Seq((1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2))
    println("spark.createDataFrame data的长度大于sc.defaultParallelism时，长度：" + data.length + " 分区数：" + spark.createDataFrame(data).rdd.getNumPartitions)

    println(spark.createDataFrame(data).limit(100).rdd.getNumPartitions)
    println(spark.createDataFrame(data).distinct().rdd.getNumPartitions)
    spark.stop
  }
}
