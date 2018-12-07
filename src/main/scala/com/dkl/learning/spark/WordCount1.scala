package com.dkl.learning.spark

import org.apache.spark.SparkContext

object WordCount1 {
  def main(args: Array[String]): Unit = {

    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "WordCount1")

    val path = "files/abc.txt"
    val hdfs_path = "hdfs://ambari.master.com:8020/tmp/dkl/abc.txt"
    val rdd1 = sc.textFile(hdfs_path, 4)
    val words = rdd1.flatMap(x => x.split(" "))
    val result = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)

    result.foreach(println)
    //    result.sortByKey().foreach(f => println(f._1, f._2))
    //    println(result.count())
    //    println(result.sortByKey().collect().mkString(","))
  }
}
