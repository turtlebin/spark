package ScalaGrammarTest

import scala.collection.mutable.Map

object GetOrElseUpdate extends App{

  val a=scala.collection.mutable.Map[Char,Int]()
  a.getOrElseUpdate('c', 1)
  a.put('a', 2)
  a+=(('b',3),('d',4))
  a.foreach(println)
  def returnInt(c:Char):Int={
    1
  }
}