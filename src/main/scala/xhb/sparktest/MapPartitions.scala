package xhb.sparktest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner

object MapPartitions extends App{
     val sc = new SparkContext(new SparkConf()
    .setAppName("Accumulator1")
    .setMaster("local"))
 
//val a = sc.parallelize(1 to 9, 3)
//  def doubleFunc(iter: Iterator[Int]) : Iterator[(Int,Int)] = {
//    var res = List[(Int,Int)]()
//    while (iter.hasNext)
//    {
//      val cur = iter.next;
//      res .::= (cur,cur*2)
//    }
//    res.iterator
//  }
//val result = a.mapPartitions(doubleFunc)
//println(result.collect().mkString)
     
     val mapList=List(1->"test1",2->"test2",3->"test3",1->"test11")
     val a=sc.parallelize(mapList, 3).partitionBy(new HashPartitioner(3))
//     println(a.partitioner.getOrElse("none"))
     
     
//val rdd = sc.parallelize(1 to 8,3)
//rdd.mapPartitionsWithIndex{
//    (partid,iter)=>{
//        var part_map = scala.collection.mutable.Map[String,List[Int]]()
//        var part_name = "part_" + partid
//        part_map(part_name) = List[Int]()
//        while(iter.hasNext){
//            part_map(part_name) :+= iter.next()//:+= 列表尾部追加元素
//        }
//        part_map.iterator
//    }
//}.collect
     
     
}