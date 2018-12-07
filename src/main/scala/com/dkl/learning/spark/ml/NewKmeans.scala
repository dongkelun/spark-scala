package com.dkl.learning.spark.ml
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.ml.clustering.KMeans
object NewKmeans {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("NewKmeans")
      .master("local")
      .getOrCreate()
    val data_path = "files/ml/linear_regression_data3.txt"
    import spark.implicits._
    val dataFrame = spark.read.format("libsvm").load(data_path)

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    val scalerModel = scaler.fit(dataFrame)

    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()
    val Array(train, test) = scaledData.randomSplit(Array(0.8, 0.2))

    // 训练k-means模型
    val kmeans = new KMeans().setK(2).setSeed(1L)
    kmeans.setFeaturesCol("scaledFeatures")
    val model = kmeans.fit(train)

    // 评估聚类通过计算在平方误差的总和。
    val WSSSE = model.computeCost(train)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // 显示结果
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    model.transform(train).show()
    model.transform(test).show()

  }
}
