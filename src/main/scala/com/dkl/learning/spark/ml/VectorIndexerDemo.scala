package com.dkl.learning.spark.ml

import org.apache.spark.sql.SparkSession

object VectorIndexerDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val data_path = "files/ml/featureprocessing/data1.txt"
    import org.apache.spark.ml.linalg.Vectors
    import org.apache.spark.sql.Row
    val data = spark.read.text(data_path).map {
      case Row(line: String) =>
        var arr = line.split(',')
        (arr(0), Vectors.dense(arr(1).split(' ').map(_.toDouble)))
    }.toDF("label", "features")
    data.show(false)
    import org.apache.spark.ml.feature.VectorIndexer
    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(5)

    val indexerModel = indexer.fit(data)

    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} categorical features: " +
      categoricalFeatures.mkString(", "))

    val indexedData = indexerModel.transform(data)
    indexedData.show(false)
  }
}
