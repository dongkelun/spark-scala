package com.dkl.scalabasic

object CaseClassTest {
  val Name(first, scend, third) = "haha hehe lala"; // first,second,third必须是未声明的变量
  println(first)
  println(scend)
  println(third)
  val str = "asd sdf gh sdf"
  str match {
    case Name(a, b, c) => println("3 param:" + a + "," + b + "," + c)
    case Name(a, b, c, d) => println("4 param:" + a + "," + b + "," + c + "," + d)
  }

  val currency: Money = RMB(12.3, "yuan")
  //多态下，子类的模式匹配
  currency match {
    case Dollar(x: Double) => println("dollar:" + x)
    case RMB(x: Double, y: String) => println("rmb:" + x + y) //rmb:12.3yuan
  }

  val currency2 = RMB(23.4, "yuan")
  println(currency2.copy(value = 12.1)) //RMB(12.1,yuan)  =>自动产生的toString

  /**
   * 匹配循环嵌套的样例类
   * （1）循环嵌套的样例类：一个case class的对象中，包含另一个case class的对象实例
   * （2）因为对象存在循环嵌套，则需要使用递归处理对象，且对象要有别名，用于递归处理。name @ pattern
   */
  val bundle: Item = Bundle("Father's day special", 20.0, // 这个对象包含2个Item，1个是artice，另一个是包含两个artice的Bundle
    Article("scala for impatient", 39.3),
    Bundle("other lanugage", 10.0, Article("thinking in java", 79.5), Article("c++ progeamme", 65.4)))

  def price(it: Item): Double = it match {
    case Article(_, price) => price
    case Bundle(_, disc, items @ _*) => items.map(price(_)).sum - disc
  }
// test PR3
  println(price(bundle))
  def main(args: Array[String]): Unit = {

  }
}

object Name {
  //当用未初始化的变量放在一个类()里,和等式右侧的对象进行匹配时,则这些未初始化的变量调用该类的unapply方法进行初始化
  //  def unapply(input: String): Option[(String, String, String)] = { //元祖：不同类型的值的集合
  //    if (input.indexOf(" ") == -1)
  //      None
  //    else
  //      Some(input.split(" ")(0), input.split(" ")(1), input.split(" ")(2))
  //  }

  /**
   * unapply用来提取固定个数的变量，来给未知变量赋值.若要提取出不定长度的变量，用unapplySeq方法
   * unapplySeq与unapply不能同时存在，否则模式匹配时，只会调用unapply进行匹配
   */
  def unapplySeq(input: String): Option[Seq[String]] = {
    if (input.indexOf(" ") == -1)
      None
    else
      Some(input.split(" "))
  }
}

abstract class Money
case class Dollar(value: Double) extends Money
case class RMB(value: Double, danwei: String) extends Money

abstract class Item
case class Article(description: String, price: Double) extends Item
case class Bundle(description: String, discount: Double, iterms: Item*) extends Item
