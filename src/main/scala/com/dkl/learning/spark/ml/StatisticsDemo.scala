package com.dkl.learning.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext

object StatisticsDemo {
  val conf = new SparkConf().setAppName("OldSparkHiveDemo")
  val sc = new SparkContext(conf)
  val data_path = "src/main/resources/scala/ml/sample_stat.txt"
  println(sc.textFile(data_path).count())
  def main(args: Array[String]): Unit = {
    newStat()
  }

  def oldStat() {
    val conf = new SparkConf().setAppName("OldSparkHiveDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data_path = "src/main/resources/scala/ml/sample_stat.txt"

    val data = sc.textFile(data_path).map(_.split("\t")).map(f => f.map(f => (f.toDouble)))
    data.count()

    val data1 = data.map(f => Vectors.dense(f))
    val stat1 = Statistics.colStats(data1)
    println(stat1.max)
    stat1.variance
    stat1.normL1
    stat1.normL2
  }

  def newStat() {
    val spark = SparkSession.builder().appName("CsvDemo").master("local").getOrCreate()
    val data_path = "src/main/resources/scala/ml/sample_stat.txt"
    import spark.implicits._
    import org.apache.spark.ml.linalg.Vector
    //    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    val df = spark.read.text(data_path).map {
      case Row(line: String) =>
        var arr = line.split("\t").map(_.toDouble)
        (arr(0), arr(1), arr(2), arr(3), arr(4))
    }
    //    val stat1 = Statistics.colStats(dfRdd.map(f => Vectors.dense(f)))
    //    println(stat1.max)
    //println(df.count())
    df.printSchema()
    df.show()
    var dfr = df.describe()
    dfr.printSchema()
    dfr.show()
    dfr.where("summary='count'").show()
    println(dfr.where("summary='count'"))

  }

}
