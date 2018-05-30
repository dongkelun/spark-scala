package com.dkl.leanring
import org.apache.spark.ml.feature.Bucketizer
import com.hs.xlzf.Utils.SparkUtil

object BucketizerDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.init_spark("BucketizerDemo")
    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val data = Array(-0.6, -10.3, 0.0, 10.2)
    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    dataFrame.show()
    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(dataFrame)
    bucketedData.show()
  }

}