//package sparkKafka
//
//import org.apache.spark.{ SparkConf, SparkContext }
//import org.apache.spark.streaming.{ Seconds, StreamingContext }
//import org.apache.spark.streaming.dstream.{ DStream, ReceiverInputDStream }
//import scala.collection.immutable
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.streaming.kafka010._
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
////todo:利用sparkStreaming对接kafka实现单词计数----采用receiver(高级API)
//object SparkStreamingKafka_Receiver {
//  def main(args: Array[String]): Unit = {
//    //1、创建sparkConf
//    val sparkConf: SparkConf = new SparkConf()
//      .setAppName("SparkStreamingKafka_Receiver")
//      .setMaster("local[2]")
//      .set("spark.streaming.receiver.writeAheadLog.enable", "true") //开启wal预写日志，保存数据源的可靠性
//    //2、创建sparkContext
//    val sc = new SparkContext(sparkConf)
//    sc.setLogLevel("WARN")
//    //3、创建StreamingContext
//    val ssc = new StreamingContext(sc, Seconds(5))
//
//    //设置checkpoint
//    ssc.checkpoint("./Kafka_Receiver")
//    //4、定义zk地址
//    val zkQuorum = "localhost:2181"
//    //5、定义消费者组
//    val groupId = "spark_receiver"
//    //6、定义topic相关信息 Map[String, Int]
//    // 这里的value并不是topic分区数，它表示的topic中每一个分区被N个线程消费
//    val topics = Map("test3" -> 1)
//
//    //7、通过KafkaUtils.createStream对接kafka
//    //这个时候相当于同时开启3个receiver接受数据
//    val receiverDstream: immutable.IndexedSeq[ReceiverInputDStream[(String, String)]] = (1 to 3).map(x => {
//      val stream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)
//      stream
//    })
//    //使用ssc.union方法合并所有的receiver中的数据
//    val unionDStream: DStream[(String, String)] = ssc.union(receiverDstream)
//    //8、获取topic中的数据
//    val topicData: DStream[String] = unionDStream.map(_._2)
//    //9、切分每一行,每个单词计为1
//    val wordAndOne: DStream[(String, Int)] = topicData.flatMap(_.split(" ")).map((_, 1))
//    //10、相同单词出现的次数累加
//    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
//    //11、打印输出
//    result.print()
//
//    //开启计算
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
//
