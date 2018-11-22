package com.dkl.leanring.spark.df

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object OldDFDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Rdd2Df").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val data = Seq((7, 2, 3), (1, 8, 6), (4, 5, 9))
    val rdd = sc.parallelize(data)
    val df = rdd.toDF("col1", "col2", "col3")

    df.show()

    sc.stop

  }
}