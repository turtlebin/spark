package xhb.sparktest

object option extends App {
   val capitals = Map("France"->"Paris", "Japan"->"Tokyo", "China"->"Beijing")
   println(capitals.get("Japan"))
   println(capitals.get("non").getOrElse("jello"))
}