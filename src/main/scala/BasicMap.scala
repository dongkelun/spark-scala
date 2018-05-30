import org.apache.spark._

object BasicMap {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "BasicMap", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(List(1, 2, 3, 4))

    println(java.util.UUID.randomUUID().toString)
    val result = input.map(x => x * x)
    println(result.collect().mkString(","))
  }
}