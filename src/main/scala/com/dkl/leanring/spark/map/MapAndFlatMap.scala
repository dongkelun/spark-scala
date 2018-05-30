package com.dkl.leanring.spark.map

import org.apache.spark.sql.SparkSession

object MapAndFlatMap {
  def main(args: Array[String]): Unit = {
    //rddDemo()
    testReduceBykey
  }

  def dataFrameDemo() {
    val spark = SparkSession.builder().appName("MapAndFlatMap").master("local").getOrCreate()
    val data = Array(("AAA", 111), ("BBB", 222), ("CCC", 333))
    val data1 = Array("AAA", 111, "BBB", 222, "CCC", 333)
    //data1.tail.foreach(println)
    println(data1.slice(data1.length - 3, data1.length).mkString(" "))
    val df = spark.createDataFrame(data)
    df.show()
  }

  def testReduceBykey() {
    val spark = SparkSession.builder().appName("MapAndFlatMap").master("local").getOrCreate()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Array(("张三", "喜欢白色", 3), ("张三", "喜欢白色", 4), ("张三", "喜欢黑色", 2), ("张三", "喜欢黑色", 3)))
    val res1 = rdd.map(kv => (kv._1 + "," + kv._2, kv._3))
    val res2 = res1.reduceByKey((x, y) => x + y).map(kv => { val arr = kv._1.split(","); (arr(0), arr(1), kv._2) })
    res2.foreach(println)
  }

  def rddDemo() {
    val spark = SparkSession.builder().appName("MapAndFlatMap").master("local").getOrCreate()
    val sc = spark.sparkContext
    val arr = sc.parallelize(Array(("AAA", 111), ("BBB", 222), ("CCC", 333)))
    val arr1 = arr.flatMap(x => { println(x._1 + " " + x._2); "123456" })
    val arr2 = arr.map(x => { println(x._1 + " " + x._2); x._1 + x._2 })
    //println(arr1.count())
    //arr1.foreach(println)
    //    println(arr2.count())
    //arr2.foreach(println)
    var data = sc.parallelize(Array("A;B;C;D;B;D;C", "B;D;A;E;D;C", "A;B"))
    val temp = data.map(_.split(";")).map(x => {
      var a: IndexedSeq[(String, Int)] = IndexedSeq()
      for (i <- 0 until x.length - 1) a :+= (x(i) + "," + x(i + 1), 1)
      a
    }).map(f => f.map(f => (1, 1)))

    //temp.foreach(println)
    val data1 = sc.parallelize(Array((1, 1, 4), (2, 1, 4), (1, 3, 4), (4, 1, 4), (5, 5, 4), (6, 7, 4)))
    data1.foreach(println)
    println("**********")
    var reduce = data1.reduce((x, y) => { println(x, y); (x._1 + x._2, y._1 + y._2, y._1 + y._2) })
    println("**********")
    println(reduce)
    println("-----")
    val data2 = sc.parallelize(Array((1, 1), (2, 1), (1, 3), (4, 1), (5, 5), (6, 7)))
    val re2 = data2.reduceByKey((x, y) => { println("ssssssssssssss", x, y); x + y })
    data2.reduceByKey((x, y) => { println("ssssssssssssss", x, y); x + y }).foreach(println)
    /// println(re2.collect())
    println("-----")
    //RDD[(String, Int)]   RDD[IndexedSeq[(String, Int)]] RDD[IndexedSeq[(String, Int)]]
    val ss = "A;B;C;D;B;D;C"
    val sss = for (i <- 0 until ss.length - 1) yield (ss(i) + "," + ss(i + 1), 1)
    var a: IndexedSeq[(String, Int)] = IndexedSeq()
    a = a :+ ("abc", 1)
    a = a :+ ("abc", 2)
    a :+= ("abc", 2)
    //    a.foreach(println)
    //    println(temp.count())
    println("11111111111111")
    val temp1 = data.map(_.split(";")).flatMap(x => {
      var a: IndexedSeq[(String, Int)] = IndexedSeq()
      for (i <- 0 until x.length - 1) a :+= (x(i) + "," + x(i + 1), 1)
      a
    })
    //temp1.foreach(f => println(f.getClass))
    data.map(_.split(";")).flatMap(x => {
      var a: IndexedSeq[(String, Int)] = IndexedSeq()
      for (i <- 0 until x.length - 1) a :+= (x(i) + "," + x(i + 1), 1)
      a
    }).reduceByKey(_ + _).foreach(println)
  }

}