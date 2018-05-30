package com.dkl.leanring.spark.map

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.ArrayList
object RddMerge {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RddMerge").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array(1001, 1002)).collect()
    val rdd2 = sc.parallelize(Array(2001, 2002, 2003, 2004)).collect()
    val mapAdd2 = (rdd1 /: rdd2)((arr1, arr2) => {
      println(arr1(0), arr2)
      arr1
    })

  }
}