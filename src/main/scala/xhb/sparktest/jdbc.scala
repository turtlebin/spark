package xhb.sparktest

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
object jdbc {
  def main(args:Array[String]){
  val spark = SparkSession
  .builder()
  .appName("Spark Hive Example")
  .config("driver-memory","2g")
  .config("executor-memory","6g")
  .config("spark.default.parallelism","4")
  .master("yarn")
  .getOrCreate()
import spark.implicits._
import spark.sql
//val jdbcDF = spark.read
//  .format("jdbc")
//  .option("url", "jdbc:mysql://192.168.0.168:3306/test")
//  .option("dbtable", args(0))
//  .option("driver","com.mysql.jdbc.Driver")
//  .option("user", "root")
//  .option("password", "csasc")
//  .load()
//  jdbcDF.persist(StorageLevel.MEMORY_AND_DISK)
//  jdbcDF.show()
//  println(jdbcDF.rdd.partitions.size)
//println(jdbcDF.count())


val connectionProperties = new Properties()
connectionProperties.put("user", "root")
connectionProperties.put("password", "csasc")
connectionProperties.put("driver","com.mysql.jdbc.Driver")
val df=spark.read.jdbc("jdbc:mysql://192.168.31.227:3306/test", args(0), "id", 1, 4000000,20,connectionProperties)
df.persist(StorageLevel.MEMORY_AND_DISK)
df.show()
val df2=spark.read.jdbc("jdbc:mysql://192.168.31.227:3306/test", args(0), "id", 1, 4000000,20, connectionProperties)
df2.persist(StorageLevel.MEMORY_AND_DISK)
df2.show()
println(df2.rdd.getNumPartitions)
val df3=df.join(df2,"id")
df3.persist(StorageLevel.MEMORY_AND_DISK)
df3.show()
println(df3.count())
println(df3.rdd.getNumPartitions)

//对于这段代码有很多可圈可点的地方。
//1.persists顺序，一开始，缓存df1的20个分区，全部都能够放在内存中
//2.然后缓存df2的20个分区，只能够缓存一部分在内存中，内存不足的部分被输出到磁盘中.
//3.最后执行连接和缓存，此时df1的全部分区缓存从内存被输出到磁盘中，而内存中只保留df3的数据
//执行count方法时直接从内存读取df3的数据，因此可以很快得出结果
//一般来说，在2Gexector内存，4core的情况下。每个分区处理200000条数据的话能够大大降低GC操作的时间
//实际测试：4000000条数据，加载成DF时：分4个分区（已接近极限，估计分更小分区的话必定会发生OOM）时所需时间为5.8min(5.0min),分8个分区时所需时间为54s(17s），分12个分区时40s（12s）
//分区大小越大，一次处理的数据量越小，GC操作时间越小
//而使用上面的load方法加载数据库数据，将没办法制定分区，这样会导致所有数据都挤进一个task，导致单CPU处理的数据量过大，内存溢出
//此处有点疑问，因为分4个分区由4个cores并行计算，使用的总内存为2G（executor-memory）此时勉强可以计算，而只放进一个core却不能计算
//这究竟是什么原因，同一个executor中每个core能使用的内存是平均分配的还是动态分配的？
//另外分8个分区由4个cores并行计算时，却能够把400W行数据全部加载入内存中，而且速度非常快,这究竟又是什么原因
//虽然有部分不明白的事情，但基本可以确定的是，尽可能大的提高分区数，可以大大减少GC时间，而分区数太小则可能导致GC时间太长从而发生OOM问题
//load方法报错时主要是	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:324)开始的
//另外很重要的一点是：当调用show方法的时候即使使用了persist，也不会把所有数据全部缓存，因为show方法不需要使用到全部数据，对于没一个DF，大概只缓存了7M的数据，全部数据为150M左右
//而由于设置了persist，在连接的时候仍然是会把所有数据缓存至内存中（或磁盘中），也就是说，实际上是用到的对应RDD的全部的时候才会真正缓存该RDD的全部数据
//实际上对于只使用一次的RDD，是不需要persis的，因为persist并不会把不能完成的任务变成能完成。
//就如同上例，即使不persist，最后的结果仍然能很快跑出来，且不会报错
//结论：spark 处理大数据时遇到OOM，GC时间过长时应从分区数，cores数入手，cores越大，同时处理的分区就越多，需要的内存越大，GC可能性越高。分区数越大，同时处理的分区数据量越小，需要的内存越小，GC可能行越小。
//  .jdbc("jdbc:mysql://222.201.145.241:3306/jcfxdb", "ta_jxgl_cj", connectionProperties)
//    val studentRDD=spark.sparkContext.parallelize(Array("3 小翠 G 27","4 小狗蛋 B 50"))
//                      .map(x=>x.split(" "))
//    val ROWRDD=studentRDD.map(x=>Row(x(0).toInt,x(1).trim,x(2).trim,x(3).toInt))
//       ROWRDD.foreach(print)
//    //设置模式信息
//    val schema=StructType(List(StructField("id",IntegerType,true),StructField("name",StringType,true),StructField("gender",StringType,true),StructField("age", IntegerType, true)))
//
//    val studentDF=spark.createDataFrame(ROWRDD,schema)
//
//    val parameter=new Properties()
//    parameter.put("user","root")
//    parameter.put("password","csasc")
//    parameter.put("driver","com.mysql.jdbc.Driver")
//    val jdbcDF2 = spark.read.jdbc("jdbc:mysql://master:3306/test", "test2", parameter)
//    jdbcDF2.show()
//    studentDF.write.mode("append").jdbc("jdbc:mysql://master:3306/test","test2",parameter) //******"是数据库名，/*/*/*/*/*表名
//   //jdbcDF.show()
}
}