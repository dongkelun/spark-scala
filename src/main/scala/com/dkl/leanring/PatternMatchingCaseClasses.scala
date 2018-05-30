package com.dkl.leanring

case class Passenger(
  first: String, last: String)
case class Train(
  travelers: Vector[Passenger],
  line: String)
case class Bus(
  passengers: Vector[Passenger],
  capacity: Int)

object PatternMatchingCaseClasses {
  def travel(transport: Any): String = {
    transport match {
      case Train(travelers, line) =>
        s"Train line $line $travelers"
      case Bus(travelers, seats) =>
        s"Bus size $seats $travelers"
      case Passenger => "Walking along"
      case _ => s"$transport is in limbo!"
    }
  }

  def main(args: Array[String]): Unit = {
    val i:Int =100 
    var p = Passenger("2","3")
    print(travel(Int))
  }
}