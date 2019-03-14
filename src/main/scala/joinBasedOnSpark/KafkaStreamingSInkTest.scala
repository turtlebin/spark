package joinBasedOnSpark

import java.util.Properties

import scala.collection.mutable.ListBuffer

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies

object KafkaStreamingSinkTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KafkaStreamingSink").setMaster("spark://master:7077")
    val ssc = new StreamingContext(sparkConf, Minutes(10))
    //  ssc.checkpoint("hdfs://master:9000/checkpoint")
    ssc.checkpoint("hdfs://master:9000/checkpoint")
    val topicsSet = "join3".split(",").toSet
    val topicsSet2 = "join4".split(",").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "master:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", "master:9092")
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

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

    rowsDStream.print()
    rowsDStream.foreachRDD(rdd => println("rdd的分区数量为:" + rdd.getNumPartitions))

    def updateFunc(values: Seq[(Int, String)], state: Option[ListBuffer[(Int, String)]]) = {
      val list = state.getOrElse(ListBuffer[(Int, String)]())
      values.foreach(x => list += x)
      Some(list)
    }
    
    val keyToList = rowsDStream.updateStateByKey(updateFunc _, new HashPartitioner(42))

    keyToList.persist(StorageLevel.MEMORY_AND_DISK)

    var i = 0
    keyToList.foreachRDD(rdd => {
      println(i)
      i += 1
    })
    //  keyToList.foreachRDD(rdd => {
    //    println(rdd.toDebugString)
    //    println("keyToList中RDD的分区数量为:"+rdd.getNumPartitions)
    //    println("rdd中数据量大小为："+rdd.first())
    //  })
    keyToList.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        partition.foreach(x => {
          kafkaProducer
            .value
            .send("update6", x._2.mkString("-"))
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}