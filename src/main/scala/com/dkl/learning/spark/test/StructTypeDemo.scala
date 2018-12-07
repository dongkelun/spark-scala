package com.dkl.learning.spark.test

import com.atomicscala.AtomicTest._
import org.apache.spark.ml.param._
import org.apache.spark.sql.types._
import org.apache.spark.ml.attribute.NominalAttribute

object StructTypeDemo extends Params {
  final val labelCol: Param[String] = new Param[String](this, "labelCol", "label column name")

  setDefault(labelCol -> "label")
  final val s = new Param[String](this, "s", "label column name")
  //  setDefault(s -> "s")
  def copy(extra: ParamMap): Params = {
    ???
  }

  val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")
  val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")
  setDefault(inputCol -> "name")
  setDefault(outputCol -> "woe1")

  val uid: String = null
  def main(args: Array[String]): Unit = {

    $(labelCol) is "label"
    //    println($(s))

    val labelColName = $(labelCol)
    val schema = StructType(
      Array(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("label", StringType, true),
        StructField("age", IntegerType, true)))
    //labelColName的值必须在  labelColName里有，否在抛出异常
    val labelDataType = schema(labelColName).dataType
    labelDataType is StringType

    println(schema.fields(0))
    val existingFields = schema.fields
    var outputFields = existingFields

    outputFields.foreach(println)

    val inputColNames = Array($(inputCol))
    val outputColNames = Array($(outputCol))
    inputColNames.zip(outputColNames).foreach {
      case (inputColName, outputColName) =>
        require(
          existingFields.exists(_.name == inputColName),
          s"Iutput column ${inputColName} not exists.")
        require(
          existingFields.forall(_.name != outputColName),
          s"Output column ${outputColName} already exists.")

        println(outputColName)
        val attr = NominalAttribute.defaultAttr.withName(outputColName)
        println(attr)
        println(attr.toStructField())
        outputFields :+= attr.toStructField()
    }

    println("**********************************************")
    outputFields.foreach(println)
    StructType(outputFields)

  }

}
