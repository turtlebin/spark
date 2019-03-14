package BroadcastTest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.unsafe.memory.MemoryLocation

object broadcastTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("broadcastTest").setMaster("spark://master:7077")
    val sc = new SparkContext(sparkConf)
    val broadcast = sc.broadcast(List(1, 2, 3))
    val test = sc.parallelize(Seq(1, 2, 3, 4, 5, 6))
    val test2 = test.map(x => broadcast.value.contains(x)).map(x=>(x,1)).reduceByKey(_+_)
    test2.checkpoint()
    test2.cache()
    test2.foreach(println)
    //     test.foreach(
    //         x=>println(broadcast
    //         .value
    //         .contains(x)))
    sc.stop()
  }
}