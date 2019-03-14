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
import org.apache.spark.HashPartitioner

object StreamUnionTest extends App{
  
   val sparkConf = new SparkConf().setAppName("DirectKafkaJoin").setMaster("local[4]")
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  ssc.checkpoint("./Kafka_Receiver")

  val topicsSet = "test1".split(",").toSet
  val topicsSet2= "test2".split(",").toSet
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)) //  val messages: DStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

  val messages = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    
   val messages2 = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topicsSet2, kafkaParams))
    
    messages.map(_.value()).print()
    messages2.map(_.value()).print()
//    
//    val data1=messages.map(x=>x.value())
//    val data2=messages2.map(x=>x.value())
//    
//    
    val union=messages.union(messages2)
    val s=union.map(x=>x.value().toString)
    s.print()
    union.foreachRDD(rdd=>{
      val rdd2=rdd.map(x=>(x.value().toString().split(",")(0),x.value()))
      val rdd3=rdd2.partitionBy(new HashPartitioner(8))
      println(rdd3.partitioner.get)
      println(rdd3.toDebugString)
      println(2)
      rdd3.foreachPartition(partition=>println(1))
      })//这样是不行的，因为此处的rdd为ConsumerRecord，实际上 并不能序列化，而foreachPartition会把RDD作为参数传入，此时会出现序列化失败的错误
    
  ssc.start()
  ssc.awaitTermination()
}