
object Main extends App {
  implicit val iterator: Iterator[Int] = List(1, 2).toIterator

  def i(implicit it: Iterator[Int]) = {
    it.next()
  }

  println(i == 1 && i == 2)
}