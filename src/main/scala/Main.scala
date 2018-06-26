import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import java.text.DecimalFormat

object Main extends App {

  def test1() {
    implicit val iterator: Iterator[Int] = List(1, 2).toIterator

    def i(implicit it: Iterator[Int]) = {
      it.next()
    }

    println(i == 1 && i == 2)

  }

  def testMap() {

    val map1 = Map("dkl" -> 1, "age" -> 30, "addr" -> "ping", ("col1", 4), ("2", 2))
    val map2 = map1 + ("1" -> 1) + (("3", 2))
    val map3 = scala.collection.mutable.Map("33" -> 3)
    println(map1.getClass)
    println(map3.getClass)
    println(map1)
    println(map2)

  }
  import scala.collection.mutable.ArrayBuffer
  val arr = ArrayBuffer(1, 2)
  arr += 1
  println(arr.length)

  val spark = SparkSession.builder().master("local").getOrCreate()
  val sc = spark.sparkContext
  val rdd = sc.makeRDD(Seq(1, 2, 3))
  rdd.foreach(p => {

    arr += 2
    println("foreach:" + arr.length)
  })

  println("after:" + arr.length)

  def transformat(date: String, pattern: String): String = {
    import java.lang.Long
    val myformat = new SimpleDateFormat(pattern)
    myformat.setTimeZone(TimeZone.getTimeZone("GMT" + 8))
    val time = new Date(Long.valueOf(date) * 1000L)
    myformat.format(time)
  }
  println(convertDateStr2TimeStamp("2018-06-01", "yyyy-MM-dd"))
  import org.joda.time.DateTime

  val dateStr = "2018-06-01"
  val pattern = "yyyy-MM-dd"

  val date = new SimpleDateFormat(pattern).parse(dateStr)
  val dateTime = new DateTime(date)
  println(date)
  println(dateTime)
  println(new SimpleDateFormat("yyyyMMdd").format(date))

  println(date)
  def convertDateStr2TimeStamp(dateStr: String, pattern: String): Long = {
    new SimpleDateFormat(pattern).parse(dateStr).getTime
  }
  import org.joda.time.DateTime
  def convertDateStr2Date(dateStr: String, pattern: String): DateTime = {
    new DateTime(new SimpleDateFormat(pattern).parse(dateStr))
  }

  //  println(convertDateStr2Date("2018-06-01", "yyyy-MM-dd"))
  //  println(dateStrAddDays2TimeStamp("2018-06-01", "yyyy-MM-dd", 30))
  //时间字符串+天数=>时间戳
  def dateStrAddDays2TimeStamp(dateStr: String, pattern: String, days: Int) = {
    convertDateStr2Date(dateStr, pattern).plusDays(days).toString()
  }
  def dateStrAddYears2TimeStamp(dateStr: String, pattern: String, years: Int) = {
    convertDateStr2Date(dateStr, pattern).plusYears(years).toString()
  }
  //  val start = convertDateStr2TimeStamp("2018-02-28", "yyyy-MM-dd")
  //
  //  val end = convertDateStr2TimeStamp("2019-02-27", "yyyy-MM-dd")
  //  println(end - start)
  //  println((end - start) / 1000 / 3600 / 24 / 365)
  //  println(convertTimeStamp2Minute(end - start))
  def convertTimeStamp2Minute(timestamp: Long) = {
    new DateTime(timestamp).year().getAsString.toInt
  }

  //  val year = datesDiffYears("2018-02-28", "2019-02-28", "yyyy-MM-dd")
  def datesDiffYears(dateStr1: String, dateStr2: String, pattern: String) = {
    val start = convertDateStr2TimeStamp(dateStr1, pattern)
    val end = convertDateStr2TimeStamp(dateStr2, pattern)
    (end - start) / 1000 / 3600 / 24 / 365
  }

  //  val df1 = spark.createDataFrame(Array((1, 2, 3), (4, 5, 6))).toDF("col1", "col2", "col3")
  //
  //  val df2 = spark.createDataFrame(Array((7, 8, 9), (10, 11, 12))).toDF("col1", "col2", "col3")
  //  val df3 = spark.createDataFrame(Array((13, 14, 15))).toDF("col1", "col2", "col3")
  //
  //  df2.show
  //  df1.union(df2).union(df3).show
  //  df1.show()
  //  df1.join(df2).show()

  println(date.getTime)
  val startDateStr = "2018-03-21"
  val endDateStr = "2018-03-22"
  val startDate = new SimpleDateFormat(pattern).parse(startDateStr)
  val endDate = new SimpleDateFormat(pattern).parse(endDateStr)
  val between = endDate.getTime - startDate.getTime
  val second = between / 1000
  //  val hour = between / 1000 / 3600
  val day = between / 1000 / 3600 / 24
  val year = between / 1000 / 3600 / 24 / 365
  println(second)
  val hour: Float = between.toFloat / 1000 / 3600
  val decf: DecimalFormat = new DecimalFormat("#.00")
  println(hour)
  println(decf.format(hour))
  println(year)
  println(endDate.getTime - startDate.getTime)

}