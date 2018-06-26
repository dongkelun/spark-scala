package com.dkl.leanring.scala

import scala.io.Source
import scala.util.parsing.json.JSON
import scala.xml.XML
import java.io.PrintWriter
import org.apache.spark.sql.SparkSession

object TestScalaUrl {
  def main(args: Array[String]) {
    //获取抽样参数的url
    //    val url = "http://restapi.amap.com/v3/direction/driving?origin=116.9905,36.67085&destination=120.312643,36.67085&extensions=all&output=xml&key=1c9ebb6d1d1fca0f3aa97e58f1515a45&strategy=19"
    //    //链接url，获取返回值
    //    val fileContent = Source.fromURL(url, "utf-8").mkString
    //    //写入本地磁盘
    //    //    println(fileContent)
    //    val pw = new PrintWriter("D:/map.xml")
    //    pw.write(fileContent)
    //    pw.flush
    //    pw.close
    //    testArray
    val xmlPath = "D:/map.xml"
    val polylines = getPolyline(xmlPath)

    polylines.foreach(polyline => getPolylineDistance(polyline))
    println("==========================================")
    val xml = XML.load("D:/map.xml")
    val polyline = xml \\ "polyline"
    val arr = polyline.map(one => one.text).flatMap(_.split(";")).distinct
    getPolylineDistance(arr)
    var count = 0
    polylines.foreach(count += _.length)

    println(count, arr.length - 1)

  }

  /**
   * 获取每个路线上的经纬度
   */
  def getPolyline(xmlPath: String) = {
    val spark = SparkSession.builder().appName("xml").master("local").getOrCreate()
    val xml = XML.load(xmlPath)
    val paths = xml \\ "path" //一般会有三条路线

    //多个经纬度组成的路线
    val polylines = paths.map(path => {

      //解析每个step的第一级polyline，不解析下一级，因为和第一级是重复的
      val polyline = (path \\ "step" \ "polyline")

      //以；切分为多个经纬度组成的路线，然后去重，因为上一个路线的最后一个经纬度会和下一个路线经纬度相同
      val longitudeAndLatitudes = polyline.map(one => one.text).flatMap(_.split(";")).distinct
      longitudeAndLatitudes
    })
    //打印每种路线的经纬组成的字符串
    //由于每个路线组成的字符串长度太大，所以在IDE里打印出来显示有问题，这里只做代码示例，实际可以保存到数据库里
    println("打印每种路线的经纬组成的字符串")
    polylines.foreach(polyline => println(polyline.mkString(";")))

    //polylines转为rdd，便于后面的算子操作
    val polylinesRdd = spark.sparkContext.parallelize(polylines, 10)

    //每个经纬出现的次数，然后降序排序，比如有三条路线，则根据出现三次的经纬数所占的比例可以看出，三种路线重叠的路段占整个路段的比例
    val polylinesCountSort = polylinesRdd.flatMap(kv => kv).map((_, 1)).reduceByKey(_ + _).map(kv => (kv._2, kv._1)).sortByKey(false)

    //有多少条路线
    val len = paths.length

    //每个路线都存在的经纬
    val count = polylinesCountSort.filter(_._1 == len).count()
    println("每个路线都存在的路段数所占比例")
    polylines.foreach(polyline => println(count * 1.0 / polyline.length))

    spark.stop()
    polylines
  }

  /**
   * 计算每个路线相邻的两个经纬度之间的距离，并降序排序
   */
  def getPolylineDistance(arr: Seq[String]) = {

    val distance: Array[Double] = new Array(arr.length - 1)
    //key为两个经纬度 value为对应的距离
    var mapDistance: Map[String, Double] = Map()
    for (i <- 0 until distance.length) {
      val before = arr(i).split(",")
      val after = arr(i + 1).split(",")
      distance(i) = getDistance(before(0).toDouble, before(1).toDouble, after(0).toDouble, after(1).toDouble)
      mapDistance += (arr(i) + " " + arr(i + 1) -> getDistance(before(0).toDouble, before(1).toDouble, after(0).toDouble, after(1).toDouble))
    }
    import scala.collection.immutable.ListMap

    //降序排序
    val mapDistanceSort = ListMap(mapDistance.toSeq.sortWith(_._2 > _._2): _*)

    println("降序排序打印相邻经纬和距离")
    mapDistanceSort.take(5).foreach(println)

    println("降序排序打印相邻距离")
    distance.sortWith(_ > _).take(5).foreach(println)

    println("大于200米的路段所占比例")
    println(distance.filter(_ > 200).length * 1.0 / distance.length)
  }
  /**
   * 计算两位经纬之间的距离
   */
  def getDistance(longitude1: Double, latitude1: Double, longitude2: Double, latitude2: Double) = {

    val Lat1 = rad(latitude1); // 纬度
    val Lat2 = rad(latitude2);
    val a = Lat1 - Lat2; //两点纬度之差
    val b = rad(longitude1) - rad(longitude2); //经度之差
    var s = 2 * Math.asin(Math
      .sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(Lat1) * Math.cos(Lat2) * Math.pow(Math.sin(b / 2), 2))); //计算两点距离的公式
    s = s * 6378137.0; //弧长乘地球半径（半径为米）
    s = Math.round(s * 10000d) / 10000d; //精确距离的数值
    s

  }

  def testArray() {

    val arr1 = new Array[Int](8)
    println(arr1)
    println(arr1.toBuffer)
    val arr2 = Array[Int](10)
    println(arr2.toBuffer)
    println(arr2.length)

  }

  def rad(d: Double) = {

    d * Math.PI / 180.00; //角度转换成弧度

  }
}