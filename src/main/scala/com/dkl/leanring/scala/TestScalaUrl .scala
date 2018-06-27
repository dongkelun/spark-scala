package com.dkl.leanring.scala

import scala.io.Source
import scala.util.parsing.json.JSON
import scala.xml.XML
import java.io.PrintWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object TestScalaUrl {

  //定义判断是否在服务区的标准距离，单位：米
  val standardDistance = 10000

  val serviceAreas = Array("117.058308,36.73142", "116.858263,36.73142", "117.051097,36.73142", "117.077884,36.73142",
    "117.42785,36.73142", "119.908728,36.73142", "119.891278,36.946536")

  val database_url = "jdbc:mysql://10.180.29.181:3306/route_analysis?useUnicode=true&characterEncoding=utf-8"
  val user = "route"
  val password = "Route-123"
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("JdbcDemo").master("local").enableHiveSupport().getOrCreate()
    val table_route = """
      (select
      		A.id,
      		A.en_raw_name,
      		A.ex_raw_name,
      		A.province_code,
      		A.station_name as en_station_name,
      		B.station_name as ex_station_name
      from
      (
      select
      		route_freq.id,
      		route_freq.en_raw_name,
      		route_freq.ex_raw_name,
      		route_freq.province_code,
      		toll_station.station_name
      from
      		route_freq,toll_station
      where
      		route_freq.en_raw_name=toll_station.raw_name
      and
      		route_freq.province_code = toll_station.province_code
      )A,
      (
      select
      		route_freq.id,
      		route_freq.en_raw_name,
      		route_freq.ex_raw_name,
      		route_freq.province_code,
      		toll_station.station_name
      from
      		route_freq,toll_station
      where
      		route_freq.ex_raw_name=toll_station.raw_name
      and
      		route_freq.province_code = toll_station.province_code
      )B
      where A.id= B.id
      )route
      """
    val df_route = spark.read
      .format("jdbc")
      .option("url", database_url)
      .option("dbtable", table_route)
      .option("user", user)
      .option("password", "Route-123")
      .load()
    df_route.show()

    val df_gaode = spark.read
      .format("jdbc")
      .option("url", database_url)
      .option("dbtable", "gaode_station")
      .option("user", user)
      .option("password", "Route-123")
      .load()

    //    df_gaode.show()

    df_route.createOrReplaceTempView("route")
    df_gaode.createOrReplaceTempView("gaode")

    import spark.sql

    val sql_str1 = """
    select
           A.id,
           A.lng as lng1,
           A.lat as lat1,
           B.lng as lng2,
           B.lat as lat2,
           stationId1,
           stationId2
    from
    (select
            route.id,
            gaode.id as stationId1,
            lng,
            lat
      from
            route,gaode
      where
            en_station_name = station
      and
            route.province_code=gaode.province
    )A,
    (select
            route.id,
            gaode.id as stationId2,
            lng,
            lat
      from
            route,gaode
      where
            ex_station_name = station
      and
            route.province_code=gaode.province
    )B
    where A.id = B.id
    """
    val df_res = sql(sql_str1)

    sql("use route_analysis")
    import spark.implicits._
    val polylinesDf = df_res.rdd.map(row => {
      val lng1 = row.getAs[Double]("lng1")
      val lat1 = row.getAs[Double]("lat1")
      val lng2 = row.getAs[Double]("lng2")
      val lat2 = row.getAs[Double]("lat2")
      val url = s"http://restapi.amap.com/v3/direction/driving?origin=${lng1},${lat1}&destination=${lng2},${lat2}&extensions=all&output=xml&key=1c9ebb6d1d1fca0f3aa97e58f1515a45&strategy=19"
      println(url)
      //链接url，获取返回值
      val fileContent = Source.fromURL(url, "utf-8").mkString
      //写入本地磁盘
      //    println(fileContent)
      val pw = new PrintWriter("D:/map.xml")
      pw.write(fileContent)
      pw.flush
      pw.close
      val xmlPath = "D:/map.xml"

      //获取每个路线上的经纬度
      val polylines = getPolyline(xmlPath)(0).mkString(";")

      val stationId1 = row.getAs[String]("stationId1")
      val stationId2 = row.getAs[String]("stationId2")
      (stationId1 + "_" + stationId2, polylines, stationId1, stationId2, "")
    }).toDF("lineId", "points", "stationId1", "stationId2", "serviceIds")
    polylinesDf.show()
    polylinesDf.write.mode("append").saveAsTable("route_line")
    spark.stop()

    return
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

    //获取每个路线上的经纬度
    val polylines = getPolyline(xmlPath)

    polylines.foreach(polyline => getPolylineDistance(polyline))

    val serviceAreasOnPolyline = Array.fill(polylines.length)("")
    serviceAreas.foreach(serviceArea => {
      var mapDistance: Map[String, Double] = Map()
      var index = 0
      polylines.foreach(polyline => {

        val distanceArr = new Array[Double](polylines.length)
        for (i <- 0 until distanceArr.length) {
          val before = serviceArea.split(",")
          val after = polyline(i).split(",")
          distanceArr(i) = getDistance(before(0).toDouble, before(1).toDouble, after(0).toDouble, after(1).toDouble)
          mapDistance += (serviceArea + " " + polyline(i) -> getDistance(before(0).toDouble, before(1).toDouble, after(0).toDouble, after(1).toDouble))
        }
        import scala.collection.immutable.ListMap

        //降序排序
        val mapDistanceSort = sortMapByValue(mapDistance, false)

        println("降序排序打印服务区和每个路段的距离")
        println(index)
        mapDistanceSort.take(3).foreach(println)
        val arr = mapDistanceSort.toArray
        if (arr(0)._2 < standardDistance) {
          serviceAreasOnPolyline(index) += serviceArea + ";"
        }
        index += 1
      })

    })
    serviceAreasOnPolyline.foreach(println)

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
    //    val spark = SparkSession.builder().appName("xml").master("local").getOrCreate()
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

    //    //polylines转为rdd，便于后面的算子操作
    //    val polylinesRdd = spark.sparkContext.parallelize(polylines, 10)
    //
    //    //每个经纬出现的次数，然后降序排序，比如有三条路线，则根据出现三次的经纬数所占的比例可以看出，三种路线重叠的路段占整个路段的比例
    //    val polylinesCountSort = polylinesRdd.flatMap(kv => kv).map((_, 1)).reduceByKey(_ + _).map(kv => (kv._2, kv._1)).sortByKey(false)
    //
    //    //有多少条路线
    //    val len = paths.length
    //
    //    //每个路线都存在的经纬
    //    val count = polylinesCountSort.filter(_._1 == len).count()
    //    println("每个路线都存在的路段数所占比例")
    //    polylines.foreach(polyline => println(count * 1.0 / polyline.length))
    //
    //    spark.stop()
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
  def rad(d: Double) = {

    d * Math.PI / 180.00; //角度转换成弧度

  }

  /**
   * 根据map的value值进行排序,ascending是否升序，默认为true，如果为false，则降序排序
   */
  def sortMapByValue(mapDistance: Map[String, Double], ascending: Boolean = true) = {
    import scala.collection.immutable.ListMap

    if (ascending) {
      //升序
      ListMap(mapDistance.toSeq.sortWith(_._2 > _._2): _*)

    } else {
      //降序
      ListMap(mapDistance.toSeq.sortWith(_._2 < _._2): _*)
    }

  }
  def testArray() {

    val arr1 = new Array[Int](8)
    println(arr1)
    println(arr1.toBuffer)
    val arr2 = Array[Int](10)
    println(arr2.toBuffer)
    println(arr2.length)

  }

}