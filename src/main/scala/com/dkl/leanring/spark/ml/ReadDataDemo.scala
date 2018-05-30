package com.dkl.leanring.spark.ml

object ReadDataDemo {

  def main(args: Array[String]): Unit = {
    //    oldReadRdd
    //    oldReadRdd1
    newReadDf
    //    readHdfs
    //    newReadDf1
  }

  def oldReadRdd() {
    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext
    import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.mllib.regression.LabeledPoint

    val conf = new SparkConf().setAppName("LinearRegressionWithSGD").setMaster("local")
    val sc = new SparkContext(conf)
    val data_path = "files/ml/linear_regression_data1.txt"
    val data = sc.textFile(data_path)
    val training = data.map { line =>
      val arr = line.split(',')
      LabeledPoint(arr(0).toDouble, Vectors.dense(arr(1).split(' ').map(_.toDouble)))
    }.cache()
    training.foreach(println)
  }
  def parseLibSVMRecord(line: String): (Double, Array[Int], Array[Double]) = {
    val items = line.split(' ')
    val label = items.head.toDouble
    val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
      val indexAndValue = item.split(':')
      val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
      val value = indexAndValue(1).toDouble
      (index, value)
    }.unzip

    // check if indices are one-based and in ascending order
    var previous = -1
    var i = 0
    val indicesLength = indices.length
    while (i < indicesLength) {
      val current = indices(i)
      require(current > previous, s"indices should be one-based and in ascending order;"
        + s""" found current=$current, previous=$previous; line="$line"""")
      previous = current
      i += 1
    }
    (label, indices.toArray, values.toArray)
  }
  def oldReadRdd1() {
    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext
    import org.apache.spark.mllib.util.MLUtils
    val conf = new SparkConf().setAppName("LinearRegressionWithSGD").setMaster("local")
    val sc = new SparkContext(conf)
    val data_path = "files/ml/linear_regression_data2.txt"
    val training = MLUtils.loadLibSVMFile(sc, data_path)
    training.foreach(println)
    println(sc.defaultMinPartitions)
    println(sc.defaultParallelism)
    val rdd = sc.textFile(data_path, sc.defaultParallelism)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map(parseLibSVMRecord)
    //    val line = "1 1:1.9 2:2 4:2 100:3 101:6"
    //    val items = line.split(' ')
    //    val label = items.head.toDouble
    //    val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
    //      val indexAndValue = item.split(':')
    //      val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
    //      val value = indexAndValue(1).toDouble
    //      (index, value)
    //    }.unzip
    //
    //    indices.foreach(println)
    //    values.foreach(println)
    val max = rdd.map {
      case (label, indices, values) =>
        indices.lastOption.getOrElse(0)
    }.reduce(math.max) + 1
    rdd.map {
      case (label, indices, values) =>
        indices.lastOption.getOrElse(0)
    }.foreach(println)
    println(max)
  }
  def readHdfs() {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder
      .appName("LinearRegressionWithElasticNetExample")
      .master("local")
      .getOrCreate()
    import org.apache.spark.ml.linalg.Vectors
    import org.apache.spark.sql.Row
    import spark.implicits._
    val data_path = "hdfs://192.168.44.128:8888/test/test.txt"
    val data = spark.read.text(data_path)
    data.show()
    data.rdd
  }

  def newReadDf() {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder
      .appName("LinearRegressionWithElasticNetExample")
      .master("local")
      .getOrCreate()
    import org.apache.spark.ml.linalg.Vectors
    import org.apache.spark.sql.Row
    import spark.implicits._
    val data_path = "files/ml/linear_regression_data1.txt"
    val data = spark.read.text(data_path)
    val training = data.map {
      case Row(line: String) =>
        var arr = line.split(',')
        (arr(0).toDouble, Vectors.dense(arr(1).split(' ').map(_.toDouble)))
    }.toDF("label", "features")

    val temp = data.map {
      case Row(line: String) =>
        var arr = line.split(',')
        (arr(0).toDouble, Vectors.dense(arr(1).split(' ').map(_.toDouble)))
    }
    training.show()

    //    training.orderBy("label").show()
    //    training.orderBy(-training("label")).show()
    //    println(training.collect().mkString(","))

  }
  def newReadDf1() {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder
      .appName("LinearRegressionWithElasticNetExample")
      .master("local")
      .getOrCreate()
    val data_path = "files/ml/linear_regression_data2.txt"
    val data = spark.read.text(data_path)
    val training = spark.read.format("libsvm").load(data_path)
    training.show(false)

  }
}