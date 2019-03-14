import scala.reflect.ClassTag

object typeTest extends App {
  class A {}
  val a = new Array[A](3)
  def arrayMake[T: ClassTag](first: T, second: T, func: => T, func2: (Int, String) => T) = { //func: =>T含义是可以接受任何类型的输入的函数，并且该函数返回T类型
    val r = new Array[T](2)
    r(0) = first
    r(1) = second
    r
  }
  arrayMake(1, 2, (x: Int) => 1, (x: Int, y: String) => 1).foreach(println)

  def arrayMake2[T: Manifest](first: T, second: T) = {
    val r = new Array[T](2)
    r(0) = first
    r(1) = second
    r
  }

  def divideBy2(x: Int, y: Int): Either[String, Int] = {
    if (y == 0) Left("Dude, can't divide by 0")
    else Right(x / y)
  }
  //  arrayMake(1,2).foreach(println)
}