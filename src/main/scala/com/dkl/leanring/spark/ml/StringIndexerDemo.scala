package com.dkl.leanring.spark.ml

import org.apache.spark.sql.SparkSession

object StringIndexerDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .getOrCreate()
    import spark.implicits._
    import spark.implicits._

    import org.apache.spark.ml.linalg.Vectors
    import org.apache.spark.sql.Row
    val data_path = "files/ml/featureprocessing/data2.txt"
    val data = spark.read.text(data_path).map {
      case Row(line: String) =>
        var arr = line.split(',')
        (arr(0), Vectors.dense(arr(1).split(' ').map(_.toDouble)))
    }.toDF("label", "features")

    import org.apache.spark.ml.feature.StringIndexer
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      //      .setHandleInvalid("error")
      .setHandleInvalid("skip")
      .setOutputCol("indexedLabel")

    val indexerModel = labelIndexer.fit(data)

    val indexedData = indexerModel.transform(data)
    indexedData.show(false)
    import org.apache.spark.ml.feature.IndexToString
    val labelConverter = new IndexToString()
      .setInputCol("indexedLabel")
      .setOutputCol("predictedLabel")
    val predict = labelConverter.transform(indexedData)
    predict.show(false)
    val data_new_path = "files/ml/featureprocessing/data2new.txt"
    val data_new = spark.read.text(data_new_path).map {
      case Row(line: String) =>
        var arr = line.split(',')
        (arr(0), Vectors.dense(arr(1).split(' ').map(_.toDouble)))
    }.toDF("label", "features")
    val indexedData_new = indexerModel.transform(data_new)
    indexedData_new.show(false)
  }
}