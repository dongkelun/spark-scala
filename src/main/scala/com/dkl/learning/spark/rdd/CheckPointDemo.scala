package com.dkl.learning.spark.rdd

import org.apache.spark.sql.SparkSession

/**
 * Rdd CheckPoint学习
 */
object CheckPointDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    //首先设置Checkpoint路径，这回在hdfs建立文件夹
    sc.setCheckpointDir("hdfs://ambari.master.com:8020/spark/dkl/checkpoint/checkpointdemo")

    //强烈建议先把rdd持久化到内存、否则保存到文件时会触发一个新的job重新计算
    val rdd = sc.parallelize(1 to 1000, 10).cache

    //调用checkpoint，标记该RDD要Checkpoint，只是返回ReliableRDDCheckpointData
    //transformation操作，直到action操作才会执行，所以必须在一个action算子之前执行
    rdd.checkpoint()

    val rdd1 = sc.parallelize(1 to 1000, 2).cache

    rdd1.checkpoint()
    println(rdd1.max)
    println(rdd.sum)
    val rdd2 = rdd.map(i => i * 2)

    println(rdd2.getClass)
    println(rdd2.dependencies)
    //直到action算子触发job，才会将rdd持久化到对应的hdfs文件中

    println(rdd.count)

    spark.stop()
  }

}
