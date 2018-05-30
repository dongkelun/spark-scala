package com.hs.xlzf.Utils

import org.apache.spark.ml.util._
import org.apache.spark.ml.param._
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.Model
import org.apache.spark.sql.{ DataFrame, Dataset }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkException

trait WOEBase extends Params with java.io.Serializable {
  final val labelCol: Param[String] = new Param[String](this, "labelCol", "label column name")
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")
  final val inputCols: StringArrayParam = new StringArrayParam(this, "inputCols", "input column names")
  final val outputCols: StringArrayParam = new StringArrayParam(this, "outputCols", "output column names")
  def getLabelCol() = $(labelCol)
  def getInputCol() = $(inputCol)
  def getOutputCol() = $(outputCol)
  def getInputCols() = $(inputCols)
  def getOutputCols() = $(outputCols)

  final val delta: DoubleParam = new DoubleParam(this, "delta", "防止出现0值，造成除0溢出或对数无穷大，而增加的修正值")
  def getDelta() = $(delta)
  setDefault(delta -> 1, labelCol -> "label")

  protected def getInOutCols: (Array[String], Array[String]) = {
    require(
      (isSet(inputCol) && isSet(outputCol) && !isSet(inputCols) && !isSet(outputCols)) ||
        (!isSet(inputCol) && !isSet(outputCol) && isSet(inputCols) && isSet(outputCols)),
      "WOE only supports setting either inputCol/outputCol or" +
        "inputCols/outputCols.")

    if (isSet(inputCol)) {
      (Array($(inputCol)), Array($(outputCol)))
    } else {
      require(
        $(inputCols).length == $(outputCols).length,
        "inputCols number do not match outputCols")
      ($(inputCols), $(outputCols))
    }
  }

  protected def validateAndTransformSchemas(schema: StructType): StructType = {
    val labelColName = $(labelCol)
    val labelDataType = schema(labelColName).dataType
    require(
      labelDataType.isInstanceOf[NumericType],
      s"The label column $labelColName must be numeric type, " +
        s"but got $labelDataType.")

    val (inputColNames, outputColNames) = getInOutCols
    val existingFields = schema.fields
    var outputFields = existingFields
    inputColNames.zip(outputColNames).foreach {
      case (inputColName, outputColName) =>
        require(
          existingFields.exists(_.name == inputColName),
          s"Iutput column ${inputColName} not exists.")
        require(
          existingFields.forall(_.name != outputColName),
          s"Output column ${outputColName} already exists.")
        val attr = NominalAttribute.defaultAttr.withName(outputColName)
        outputFields :+= attr.toStructField()
    }
    StructType(outputFields)
  }
}

class WOE(override val uid: String)
    extends Estimator[WOEModel] with java.io.Serializable
    with WOEBase with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("WOE"))

  def setLabelCol(value: String): this.type = set(labelCol, value)
  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)
  def setDelta(value: Double): this.type = set(delta, value)

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def fit(@transient dataset: Dataset[_]): WOEModel = {
    transformSchema(dataset.schema, true)
    val delta_value = $(delta) //防止出现0值，而增加的修正
    val T = dataset.count
    //        val B = dataset.agg(sum("y")).first.getLong(0)
    val B = dataset.where($(labelCol) + " = 1").count()
    val G = T - B

    val woe_map_arr = new ArrayBuffer[Map[String, Double]]()
    val (inputColNames, outputColNames) = getInOutCols
    inputColNames.foreach {
      inputColName =>
        //T 每组总数 ；B 每组违约总数
        val gDs_t = dataset.groupBy(inputColName).agg(count($(labelCol)).as("T"), sum($(labelCol)).as("B"))

        val gDs = gDs_t.withColumn("G", gDs_t("T") - gDs_t("B"))

        val loger = udf { d: Double =>
          math.log(d)
        }
        val woe_map = gDs.withColumn("woe", loger((gDs("B") + delta_value) / (B + delta_value) * (G + delta_value) / (gDs("G") + delta_value)))
          .select(col(inputColName).cast(StringType), col("woe"))
          .collect()
          .map(r => (r.getString(0), r.getDouble(1)))
          .toMap
        woe_map_arr += woe_map
    }

    copyValues(new WOEModel(uid, woe_map_arr.toSeq).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemas(schema)
  }
}

object WOE extends DefaultParamsReadable[WOE] with java.io.Serializable {
  override def load(path: String): WOE = super.load(path)
}

class WOEModel(override val uid: String, @transient val woe_map_arr: Seq[Map[String, Double]])
    extends Model[WOEModel]
    with MLWritable with WOEBase with java.io.Serializable {
  def this(woe_map_arr: Seq[Map[String, Double]]) = this(Identifiable.randomUID("WOE"), woe_map_arr)

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setLabelCol(value: String): this.type = set(labelCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  def setDelta(value: Double): this.type = set(delta, value)

  override def copy(extra: ParamMap): WOEModel = {
    val copied = new WOEModel(uid, woe_map_arr)
    copyValues(copied, extra).setParent(parent)
  }

  import WOEModel._
  override def write: WOEModelWriter = new WOEModelWriter(this)
  def f(i: Int) = {
    if (i > 0) {
      1
    } else {
      0
    }
  }
  val udf1 = udf(f _)
  def woe(feature: String, woe_map: Map[String, Double], inputColName: String, outputColName: String) {
    val col = woe_map.get(feature)
    col match {
      case Some(n: Double) =>
      case None =>
        logError(s"Input column_${inputColName}'s value ${feature} does not exist in the WOEModel model map. " +
          "Skip WOEModel.")
    }
  }
  override def transform(dataset: Dataset[_]): DataFrame = {
    val (inputColNames, outputColNames) = getInOutCols
    transformSchema(dataset.schema)
    require(
      woe_map_arr.length == inputColNames.length,
      s"The number of input columns is not equal to the number of WOEModel model maps ")

    var df: DataFrame = dataset.toDF()
    woe_map_arr.zipWithIndex.map {
      case (woe_map, idx) =>
        val inputColName = inputColNames(idx)
        val outputColName = outputColNames(idx)
        val woer = udf { (feature: String) =>
          woe_map.get(feature) match {
            case Some(n: Double) => n
            case None =>
              throw new SparkException(s"Input column_${inputColName}'s value ${feature} does not exist in the WOEModel model map. " +
                "Skip WOEModel.")
          }
        } //.asNondeterministic() //spark 2.3加入此句
        df = df.withColumn(outputColName, woer(dataset(inputColName).cast(StringType)))
    }

    df
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchemas(schema)
  }
}

object WOEModel extends MLReadable[WOEModel] with java.io.Serializable {
  import org.apache.hadoop.fs.Path
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.JsonAST._

  implicit val format = org.json4s.DefaultFormats

  private[WOEModel] class WOEModelWriter(instance: WOEModel) extends MLWriter with java.io.Serializable {
    private case class Data(woe_map_arr: Seq[Map[String, Double]])
    override protected def saveImpl(path: String): Unit = {
      //metadataPath=path+metadata 实际就是两个字符串拼接
      val metadataPath = new Path(path, "metadata").toString

      //返回ArrayBuffer(ParamPair(uid__key,value)),所有set过的key value
      val params = instance.extractParamMap().toSeq.asInstanceOf[Seq[ParamPair[Any]]]

      //返回 JObject(List((key,JString(value)))
      val jsonParams = render(
        params.map {
          case ParamPair(p, v) =>
            p.name -> parse(p.jsonEncode(v))
        }.toList)

      /**
       * JObject(List((class,JString(instance对应的包名.类名)), (timestamp,JInt(时间戳)), (sparkVersion,JString(spark版本)),
       * (uid,JString(设置的uid)), (paramMap,paramMap的值)))
       */
      val basicMetadata = ("class" -> instance.getClass.getName) ~
        ("timestamp" -> System.currentTimeMillis()) ~
        ("sparkVersion" -> sc.version) ~
        ("uid" -> instance.uid) ~
        ("paramMap" -> jsonParams)

      /**
       * 转成json格式的String
       * {"class":"-","timestamp":,-"sparkVersion":"-","uid":"-","paramMap":{"key":"value"}}
       */
      val metadataJson = compact(render(basicMetadata))

      //将metadataJson保存到指定路径下
      sc.parallelize(Seq(metadataJson), 1).saveAsTextFile(metadataPath)

      val data = Data(instance.woe_map_arr)
      val dataPath = new Path(path, "data").toString
      //将woe_map_arr保存到指定路径下
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class WOEModelReader extends MLReader[WOEModel] with java.io.Serializable {
    private val className = classOf[WOEModel].getName
    override def load(path: String): WOEModel = {
      val metadataPath = new Path(path, "metadata").toString
      val s = sc.textFile(metadataPath, 1).first()

      val metadata = parse(s)
      val clz = (metadata \ "class").extract[String]
      val uid = (metadata \ "uid").extract[String]

      require(className == clz, s"Error loading metadata: Expected class name" +
        s" className but found class name ${clz}")

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("woe_map_arr")
        .head()
      val woe_map_arr = data.getAs[Seq[Map[String, Double]]](0)
      val instance = new WOEModel(uid, woe_map_arr)

      val params = metadata \ "paramMap"
      params match {
        case JObject(pairs) =>
          pairs.foreach {
            case (paramName, jsonValue) =>
              val param = instance.getParam(paramName)
              val value = param.jsonDecode(compact(render(jsonValue)))
              instance.set(param, value)
          }
        case _ =>
          throw new IllegalArgumentException(
            s"Cannot recognize JSON metadata: ${s}.")
      }

      instance
    }
  }

  override def read: MLReader[WOEModel] = new WOEModelReader

  override def load(path: String): WOEModel = super.load(path)
}
