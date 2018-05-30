
object ForDemo {
  def main(args: Array[String]): Unit = {
    var a = 0;
    // 包含10
    for (a <- 1 to 10) {
      println("Value of a: " + a);
    }

    //不包含10
    for (a <- 1 until 10) {
      println("Value of a: " + a);
    }

    var b = 0;
    // for 循环
    for (a <- 1 to 3; b <- 1 to 3) {
      println("Value of a: " + a);
      println("Value of b: " + b);
    }

    val numList = List(1, 2, 3, 4, 5, 6);

    // for 循环
    for (a <- numList) {
      println("Value of a: " + a);
    }

    // for 循环
    for (
      a <- numList if a != 3 if a < 8
    ) {
      println("Value of a: " + a);

    }

    var retVal = for {
      a <- numList
      if a != 3; if a < 8
    } yield a

    // 输出返回值
    for (a <- retVal) {
      println("Value of a: " + a);
    }
  }
}