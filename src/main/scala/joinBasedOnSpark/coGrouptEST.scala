package joinBasedOnSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object coGrouptEST extends App {
  val sc = new SparkContext(new SparkConf()
    .setAppName("Accumulator1")
    .setMaster("local"))
  val rdd1=sc.parallelize(Seq(1,2,3))
  val rdd2=sc.parallelize(Seq(4,5,6))
  val rdd1Map=rdd1.zipWithIndex().map{case (x,y)=>(y,x)}
  val rdd2Map=rdd2.zipWithIndex().map{case (x,y)=>(y,x)}
  val coGroup=rdd1Map.cogroup(rdd2Map)
  println(coGroup.toDebugString)
  println(coGroup.take(3))
  val mapCoGroup=coGroup.map(x=>(x,1))
  println(mapCoGroup.toDebugString)
}