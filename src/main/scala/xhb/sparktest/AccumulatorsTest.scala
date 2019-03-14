package xhb.sparktest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class AccumulatorsTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf()
    .setAppName("Accumulator1")
    .setMaster("local"))

    val myAcc = new MyAccumulator
    sc.register(myAcc,"myAcc")
    //val acc = sc.longAccumulator("avg")
    val nums = Array("a","b","c","d","e","f","h","i")
    val numsRdd = sc.parallelize(nums,3)
    
    numsRdd.foreach(num => myAcc.add(num))
    println(myAcc)
    sc.stop()
  }
}