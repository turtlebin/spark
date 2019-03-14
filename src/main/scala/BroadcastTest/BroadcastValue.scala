package BroadcastTest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BroadCastValue {
def main(args:Array[String]):Unit={
 val conf=new SparkConf().setAppName("BroadCastValue1").setMaster("spark://master:7077")
//获取contex
val sc=new SparkContext(conf)
//创建广播变量
 val broads=sc.broadcast(Seq(3,2,1))//变量可以是任意类型
//创建一个测试的List
 val lists=List(1,2,3,4,5)
 //转换为rdd（并行化）
 val listRDD=sc.parallelize(lists)
 //map操作数据
//val results=listRDD.map(x=>x*broads.value.sum)
 val results=listRDD.map(x=>broads.value.contains(x))
 
 results.foreach(x => println("增加后的结果："+x))

//遍历结果
//results.foreach(x => println("增加后的结果："+x))
 sc.stop
 }
}

