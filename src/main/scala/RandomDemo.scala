import scala.util.Random

object RandomDemo {
  def main(args: Array[String]): Unit = {
    val r = new Random
    //0-10随机数，不包括10
    println(r.nextInt(10))
    println(r.nextInt(10))
    println(r.nextInt(10))
  } 
}