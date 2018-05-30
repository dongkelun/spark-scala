package com.dkl.leanring

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import com.hs.xlzf.Utils.SparkUtil

object VectorAssemblerDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.init_spark("VectorAssemblerDemo")
    val dataset = spark.createDataFrame(
      Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    dataset.show()
    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures"))
      .setOutputCol("features")

    
    val output = assembler.transform(dataset)
    output.show()
    println(output.select("features", "clicked").first())
  }
}