package com.dkl.leanring.spark.ml

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionModel

object OldLinearRegression {

  def main(args: Array[String]) {
    // 构建Spark对象
    val conf = new SparkConf().setAppName("OldLinearRegression").setMaster("local")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    //读取样本数据
    val data_path = "files/ml/linear_regression_data3.txt"
    val training = MLUtils.loadLibSVMFile(sc, data_path)
    val numTraing = training.count()

    // 新建线性回归模型，并设置训练参数
    val numIterations = 10000
    val stepSize = 0.5
    val miniBatchFraction = 1.0

    //书上的代码 intercept 永远为0
    //val model = LinearRegressionWithSGD.train(examples, numIterations, stepSize, miniBatchFraction)
    var lr = new LinearRegressionWithSGD().setIntercept(true)
    lr.optimizer.setNumIterations(numIterations).setStepSize(stepSize).setMiniBatchFraction(miniBatchFraction)
    val model = lr.run(training)
    println(model.weights)
    println(model.intercept)

    // 对样本进行测试
    val prediction = model.predict(training.map(_.features))
    val predictionAndLabel = prediction.zip(training.map(_.label))
    val print_predict = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }
    // 计算测试误差
    val loss = predictionAndLabel.map {
      case (p, l) =>
        val err = p - l
        err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / numTraing)
    println(s"Test RMSE = $rmse.")

    // 模型保存
    //val ModelPath = "/user/dongkelun/LinearRegressionModel"
    //model.save(sc, ModelPath)
    //val sameModel = LinearRegressionModel.load(sc, ModelPath)

  }

}
