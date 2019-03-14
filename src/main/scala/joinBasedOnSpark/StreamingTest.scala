package joinBasedOnSpark

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.Minutes
import org.apache.spark.TaskContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import scala.collection.mutable.ListBuffer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner

object StreamingTest extends App {
  val sparkConf = new SparkConf().setAppName("DirectKafkaJoin").setMaster("spark://master:7077")
  val ssc = new StreamingContext(sparkConf, Minutes(2))
//  ssc.checkpoint("hdfs://master:9000//checkpoint")
  ssc.checkpoint("hdfs://master:9000/checkpoint")
  val topicsSet = "join1".split(",").toSet
  val topicsSet2 = "join5".split(",").toSet
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "master:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean))
    //  val messages: DStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

  val messages = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

  val messages2 = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topicsSet2, kafkaParams))

  val union = messages.union(messages2)

  val rowsDStream = union.map(x => (x.value().split(",")(0), (1, x.value())))
  //    rowsDStream.foreachRDD(rdd=>{
  //      val map=scala.collection.mutable.Map[String,(Int,String)]()//初始化一个map，在driver端
  //      rdd.foreachPartition(partition=>{
  //        partition.foreach(row=>{RRRR
  //          map.getOrElseUpdate(row._1, op)
  //        })
  //      })
  //    })
  
  rowsDStream.print()
  rowsDStream.foreachRDD(rdd=>println("rdd的分区数量为:"+rdd.getNumPartitions))
//  rowsDStream.persist(StorageLevel.MEMORY_AND_DISK)
  val keyToList = rowsDStream.updateStateByKey(updateFunc _,new HashPartitioner(42))

  def updateFunc(values: Seq[(Int, String)], state: Option[ListBuffer[(Int, String)]]) = {
    val list = state.getOrElse(ListBuffer[(Int, String)]())
    values.foreach(x => list += x)
    Some(list)
  }
//  keyToList.foreachRDD(rdd=>"keyToList的分区数量为:"+rdd.getNumPartitions)
  
//  val keyToListRe=keyToList.repartition(20)
  
  keyToList.persist(StorageLevel.MEMORY_AND_DISK)
//    keyToListRe.persist(StorageLevel.MEMORY_AND_DISK)
  
  var i=0
  keyToList.foreachRDD(rdd=>{
    println(i)
    i+=1
  })
  keyToList.foreachRDD(rdd => {
//    rdd.take(10).foreach(println)
    println(rdd.toDebugString)
    println("keyToList中RDD的分区数量为:"+rdd.getNumPartitions)
    println("rdd中数据量大小为："+rdd.first())
  })
  
//  messages.foreachRDD(rdd => { //一个RDD对应一个topic
//    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //这部分在driver端执行
//    rdd.foreachPartition(partition => { //这部分在各个executor端执行
//      val o = offsetRanges(TaskContext.get.partitionId)
//      println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
//      partition.foreach(pair => {
//        //这里主要用于完成pair的处理逻辑;
//      })
//    })
//    offsetRanges.foreach { offsetRange =>
//      println("partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset + " untilOffset: " + offsetRange.untilOffset)
//      val topic_partition_key_new = offsetRange.topic + "_" + offsetRange.partition
//      println(topic_partition_key_new)
//    }
//  })

  ssc.start()
  ssc.awaitTermination()
}
