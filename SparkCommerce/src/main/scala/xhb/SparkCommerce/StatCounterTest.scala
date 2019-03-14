package xhb.SparkCommerce

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object StatCounterTest extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext(new SparkConf()
    .setMaster("local")
    .setAppName("Movie_Users_Analyzer"))
  println("stats方法调用:")
  val doubleRDD = sc.parallelize(Seq(100.1, 200.0, 300.0))
  val statCounter = doubleRDD.stats()
  println(statCounter)
  statCounter.merge(5000)
  println(statCounter)
}