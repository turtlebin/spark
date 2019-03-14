package xhb.sparktest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.shuffle.sort.SortShuffleWriter
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.ShuffleDependency

object wordCount extends App {
  val conf = new SparkConf().setAppName("wordCount").setMaster("local")
  val sc = new SparkContext(conf)
  val input = sc.textFile("/home/xhb/local/input.txt")
  val words = input.flatMap(_.split(" "))
  val counts= words.map((_,1)).reduceByKey { case (x, y) => x + y }
  //  counts.sample(withReplacement, fraction, seed)
  counts.count()
  counts.persist()
  Array.fill(3)("a")
  counts.saveAsTextFile("outputFile")
//   val rdd = sc.parallelize(1 to 10)
//    println(rdd.count())
//    val sample1 = rdd.sample(true,0.5,1)
//    sample1.collect.foreach(x => print(x + " "))
    //  val rdd1=sc.textFile("/home/xhb/local/eclipse/csv1.txt")
//  val rdd2=sc.textFile("/home/xhb/local/eclipse/csv2.txt")
//  val rdd3=rdd1.map(x=>(x,1)) 
//  val rdd4=rdd2.map(x=>(x,1))
//  val rdd5=rdd3.join(rdd4)
//  println(rdd5.toDebugString)
}