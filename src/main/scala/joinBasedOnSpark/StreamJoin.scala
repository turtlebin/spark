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

object StreamJoin extends App {
  val sparkConf = new SparkConf().setAppName("DirectKafkaJoin").setMaster("local[4]")
  val ssc = new StreamingContext(sparkConf, Minutes(1))
  ssc.checkpoint("./Kafka_Receiver")

  val topicsSet = "joinTest_1".split(",").toSet
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "master:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)) //  val messages: DStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

  val messages = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)) //使用该方法的重载方法，可以指定开始消费的偏移值
  val topicsSet2 = "joinTest_2".split(",").toSet

  val message2 = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topicsSet2, kafkaParams))

  def updateRunningSum(values: Seq[Long], state: Option[Long]) = {
    Some(state.getOrElse(0L) + values.size)
  }

  val s = messages.map(x => (x.key(), x.value()))
  s.print()

  val lines: DStream[String] = messages.map(_.value())
  val lines_2: DStream[String] = message2.map(_.value())
  val splits = lines.map(x => (x.split(",")(0), 1))
  val splits_2 = lines_2.map(x => (x.split(",")(0), 1))
  //    splits_2.print()
  val joinResult = splits.join(splits_2)
  joinResult.print()
//  println(joinResult.count())
//  messages.foreachRDD(rdd => {
//    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges//这部分在driver端执行
//    rdd.foreachPartition(partition => {//这部分在各个executor端执行
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
    val splits2=lines.map(x=>(x.split(",")(1),1L))
    val count2=splits2.updateStateByKey(updateRunningSum)
  //
  //  val count=splits.reduceByWindow(
  //      {
  //        (x,y)=>(x._1,(x._2+y._2))
  //      },
  //      {
  //        (x,y)=>(x._1,(x._2-y._2))
  //      },
  //       Seconds(5), Seconds(5))
  //
  //  count.print()
  //  count2.print()
  ssc.start()
  ssc.awaitTermination()
}