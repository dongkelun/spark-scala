import com.atomicscala.AtomicTest._
object Zipper {
  def main(args: Array[String]): Unit = {
    val left = Vector("a", "b", "c", "d")
    val right = Vector("q", "r", "s", "t")

    left.zip(right) is
      "Vector((a,q), (b,r), (c,s), (d,t))"

    left.zip(4 to 7) is
      "Vector((a,0), (b,1), (c,2), (d,3))"

    left.zipWithIndex is
      "Vector((a,0), (b,1), (c,2), (d,3))"
  }
}