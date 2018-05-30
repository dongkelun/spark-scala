package com.dkl.leanring
import org.apache.spark.ml.feature.QuantileDiscretizer

import com.hs.xlzf.Utils.SparkUtil
import org.apache.spark.sql.SQLContext

object QuantileDiscretizerDemo {

  def main(args: Array[String]): Unit = {

    val sc = SparkUtil.init_sc("QuantileDiscretizerDemo");

    val sqlContext = new SQLContext(sc)
    //    import sqlContext.implicits._
    //    sc.parallelize(Array(1, 2, 3, 4, 5)).toDF()

    val spark = SparkUtil.init_spark("DK_A")
    import spark.implicits._
    sc.parallelize(Array(1, 2, 3, 4, 5)).toDF().show()
    val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
    var df = spark.createDataFrame(data).toDF("id", "hour")
    df.show()
    val discretizer = new QuantileDiscretizer()
      .setInputCol("hour")
      .setOutputCol("result")
      .setNumBuckets(3)

    val result = discretizer.fit(df).transform(df)
    val data1 = Array((0, 18.0), (1, 19.0), (2, 8.0))
    var df1 = spark.createDataFrame(data).toDF("result", "sc")
    result.join(df1, "result").show()

    result.show()
  }
}