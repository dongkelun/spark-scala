package com.dkl.blog.spark.ml

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

/**
 * 博客：spark ML算法之线性回归使用
 * https://dongkelun.com/2018/04/09/sparkMlLinearRegressionUsing/
 */
object NewLinearRegression {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("NewLinearRegression")
      .master("local")
      .getOrCreate()
    val data_path = "files/ml/linear_regression_data3.txt"
    import spark.implicits._
    import org.apache.spark.ml.linalg.Vectors
    import org.apache.spark.sql.Row
    val training = spark.read.format("libsvm").load(data_path)

    val lr = new LinearRegression()
      .setMaxIter(10000)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lrModel = lr.fit(training)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    trainingSummary.predictions.show()

    spark.stop()
  }
}
