package joinBasedOnSpark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.RangePartitioner

object batchJoin extends App {
  val sc = new SparkContext(new SparkConf()
    .setAppName("Accumulator1")
    .setMaster("local"))
  //  val data = Array(1, 2, 3, 4, 5)
  //  val distData = sc.parallelize(data)
  //  val data2=Map[String,Integer]("1"->2)
  //  val distData2=sc.parallelize(data2.toSeq, 4)
  //  val dataMap=distData.map(x=>(x,x*x))
  //  val dataMap2=distData.map(x=>(x,x*x*x))
  //  val joinResult=dataMap.join(dataMap2)
  //  joinResult.count()
  //  println(joinResult.collect().mkString(","))

  val flight_1 = sc.textFile("/home/xhb/local/data/flight_1.csv/part*.csv",8)//这里的分区不一定是真实的分区，只是建议的分区，实际的RDD可能并不是这个分区
  println(flight_1.getNumPartitions)
  val flight_2 = sc.textFile("/home/xhb/local/data/flight_2.csv/part*.csv",8)
  println(flight_2.getNumPartitions)
  val flight_Map1 = flight_1.map(x => (x.split(",")(0), x)) //三种方式
  val flight_Map2 = flight_2.map(func)
  val flight_Map3 = flight_2.map(x => { //注意此处的大括号不能省略
    val s = x.split(",")
    (s(0), x)
  })
  implicit val sortStringByInteger = new Ordering[String] { //对于sortByKey可创建一个隐式变量，重写compare方法
    override def compare(a: String, b: String) = a.toInt.compare(b.toInt)
  }//这个隐式变量应该放在sortBy和sortByKey函数调用之前，，否则会没有效果
  val joinResult = flight_Map1.join(flight_Map2).sortByKey()

  val joinResult2 = flight_Map1.join(flight_Map2).sortBy(x => {
    x._1
  })
  println(joinResult.toDebugString)
  
//  println(joinResult.getNumPartitions)
//  println(joinResult.take(5).mkString("\n")) //take方法只是从第一个分区中读取数据
//  println(joinResult2.take(5).mkString("\n")) //take方法只是从第一个分区中读取数据
//  println(joinResult2.getNumPartitions)
  def func(line: String) = {
    val s = line.split(",")
    (s(0), line)
  }
  def addInt(a: Int, b: Int) { //省略返回值和等于号时无返回值
    a + b
  }
}