package com.dkl.learning.spark.ml.featureprocessing

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row

object ReadData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val data_path = "files/ml/featureprocessing/data1.txt"
    val data = spark.read.text(data_path).map {
      case Row(line: String) =>
        var arr = line.split(',')
        (arr(0), Vectors.dense(arr(1).split(' ').map(_.toDouble)))
    }.toDF("label", "features")
    data.show(false)
  }
}
