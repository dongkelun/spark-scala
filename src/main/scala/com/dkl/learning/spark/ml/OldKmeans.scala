package com.dkl.learning.spark.ml
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.log4j.{ Level, Logger }
object OldKmeans {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OldLinearRegression").setMaster("local")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    val scaler = new StandardScaler(withStd = true, withMean = true)
    //读取样本数据

    val data_path = "files/ml/linear_regression_data1.txt"
    val rdd = sc.textFile(data_path)
    val data1 = rdd.map { line =>
      val arr = line.split(',')
      Vectors.dense(arr(1).split(' ').map(_.toDouble))
    }.cache()
    println(data1.collect().mkString("  "))

    val scaler1 = scaler.fit(data1)
    val data2 = scaler1.transform(data1)
    val data3 = data1.map(f => (f, scaler1.transform(f)))
    //    println(data2.collect().mkString("  "))

    println(data3.collect().mkString("  "))
    val Array(train, test) = data3.randomSplit(Array(0.7, 0.3))

    println(train.collect().mkString("  "))
    println(test.collect().mkString("  "))

    import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
    import org.apache.spark.mllib.linalg.Vectors

    //加载和解析数据

    // 使用KMeans将数据集成到2个类中
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(train.map(f => f._2), numClusters, numIterations)

    val predict = clusters.predict(test.map(f => f._2))

    test.map(f => {
      val predict = clusters.predict(f._2)
      (f._1, f._2, predict)
    }).foreach(println)
    //    val index = test.map(f => f._1).collect()
    //    println(predict.zipWithIndex().collect().mkString("  "))
    //    predict.zipWithIndex().foreach(f => println(index(f._2.toInt), f._1))
    //    println(predict.collect().mkString("  "))

  }
}
