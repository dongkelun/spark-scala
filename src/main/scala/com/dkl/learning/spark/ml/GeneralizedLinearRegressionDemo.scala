package com.dkl.learning.spark.ml

import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.sql.SparkSession

object GeneralizedLinearRegressionDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("GeneralizedLinearRegressionExample")
      .master("local")
      .getOrCreate()

    // $example on$
    // Load training data
    val dataset = spark.read.format("libsvm")
      .load("files/ml/lpsa5.txt")

    val glr = new GeneralizedLinearRegression()
      .setFamily("gaussian")
      .setLink("identity")
      .setMaxIter(10)
      .setRegParam(0.3)

    // Fit the model
    val model = glr.fit(dataset)

    // Print the coefficients and intercept for generalized linear regression model
    println(s"Coefficients: ${model.coefficients}")
    println(s"Intercept: ${model.intercept}")

    // Summarize the model over the training set and print out some metrics
    val summary = model.summary
    println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
    println(s"T Values: ${summary.tValues.mkString(",")}")
    println(s"P Values: ${summary.pValues.mkString(",")}")
    println(s"Dispersion: ${summary.dispersion}")
    println(s"Null Deviance: ${summary.nullDeviance}")
    println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
    println(s"Deviance: ${summary.deviance}")
    println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
    println(s"AIC: ${summary.aic}")
    println("Deviance Residuals: ")
    summary.predictions.show()
    summary.residuals().show()
    // $example off$

    spark.stop()
  }
}
