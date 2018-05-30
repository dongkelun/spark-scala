package com.dkl.leanring.spark.hdfs

import org.apache.hadoop.fs.Path
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST._
import com.dkl.leanring.spark.test.StructTypeDemo
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.StringArrayParam
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.DoubleParam
import org.apache.spark.sql.{ DataFrame, Dataset }
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.param.ParamPair
import org.apache.spark.ml.util.MLWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.ml.Estimator

class ModelTest(override val uid: String, val woe_map_arr: Seq[Map[String, Double]])
    extends Model[ModelTest] {
  def this(woe_map_arr: Seq[Map[String, Double]]) = this(Identifiable.randomUID("WOE"), woe_map_arr)
  final val labelCol: Param[String] = new Param[String](this, "labelCol", "label column name")
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")
  final val inputCols: StringArrayParam = new StringArrayParam(this, "inputCols", "input column names")
  final val outputCols: StringArrayParam = new StringArrayParam(this, "outputCols", "output column names")
  final val delta: DoubleParam = new DoubleParam(this, "delta", "防止出现0值，造成除0溢出或对数无穷大，而增加的修正值")

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setLabelCol(value: String): this.type = set(labelCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  def setDelta(value: Double): this.type = set(delta, value)

  setDefault(inputCol -> "name")
  setDefault(outputCol -> "woe1")
  override def copy(extra: ParamMap): ModelTest = {
    val copied = new ModelTest(uid, woe_map_arr)
    copyValues(copied, extra).setParent(parent)
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    ???
  }

  def transformSchema(schema: StructType): StructType = {
    ???
  }

}

object PathDemo extends MLWriter {

  private case class Data(woe_map_arr: Seq[Map[String, Double]])
  override protected def saveImpl(path: String): Unit = {

  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("PathDemo").master("local").getOrCreate()

    val path = "hdfs://192.168.44.128:8888/test/"
    val metadataPath = new Path(path, "metadata").toString
    println(metadataPath)
    val woe_map_arr: Seq[Map[String, Double]] = Seq(Map("key1" -> 1.1, "key2" -> 3.3, "key3" -> 5.5), Map("key1" -> 2.1, "key2" -> 4.3, "key4" -> 4.5))
    val instance = new ModelTest("dkl_uid_001", woe_map_arr)
    instance.setLabelCol("NUM1")
    val params = instance.extractParamMap().toSeq.asInstanceOf[Seq[ParamPair[Any]]]
    println(params)
    val jsonParams = render(
      params.map {
        case ParamPair(p, v) =>
          println(p.name)
          println(parse(p.jsonEncode(v)))
          p.name -> parse(p.jsonEncode(v))
      }.toList)
    println(jsonParams)
    val basicMetadata = ("class" -> instance.getClass.getName) ~
      ("timestamp" -> System.currentTimeMillis()) ~
      ("sparkVersion" -> sc.version) ~
      ("uid" -> instance.uid) ~
      ("paramMap" -> jsonParams)
    println(basicMetadata)
    val metadataJson = compact(render(basicMetadata))
    //    sc.parallelize(Seq(metadataJson), 1).saveAsTextFile(metadataPath)
    println(metadataJson)
    val data = Data(instance.woe_map_arr)
    println(instance.woe_map_arr)
    val dataPath = new Path(path, "data").toString
    println(dataPath)
    //sparkSession.createDataFrame(Seq(data)).repartition(1).write.mode(SaveMode.Overwrite).parquet(dataPath)
    sparkSession.createDataFrame(Seq(data)).show()
    val metadata = parse("""{"class":"com.dkl.leanring.spark.hdfs.ModelTest","timestamp":1522639947479,"sparkVersion":"2.2.1","uid":"dkl_uid_001","paramMap":{"inputCol":"name","outputCol":"woe1","labelCol":"NUM1"}}""")
    println(metadata)
    implicit val format = org.json4s.DefaultFormats
    val clz = (metadata \ "class").extract[String]
    val uid = (metadata \ "uid").extract[String]
    println(clz, uid)
    val data1 = sparkSession.read.parquet(dataPath)
      .select("woe_map_arr")
      .head()
    println(data1)
    val woe_map_arr1 = data1.getAs[Seq[Map[String, Double]]](0)
    println(woe_map_arr1)
    println(woe_map_arr)
    val s = sc.textFile(metadataPath, 1).first()
    val instance1 = new ModelTest(uid, woe_map_arr)
    val params1 = metadata \ "paramMap"
    println(params1)
    params1 match {
      case JObject(pairs) =>
        pairs.foreach {
          case (paramName, jsonValue) =>
            val param = instance1.getParam(paramName)
            val value = param.jsonDecode(compact(render(jsonValue)))
            println(paramName + " " + param + "  " + value)
            instance1.set(param, value)
        }
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot recognize JSON metadata: ${s}.")
    }
  }
}