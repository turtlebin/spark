//package SparkKafka
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.streaming.kafka010._
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//
////todo:利用sparkStreaming对接kafka实现单词计数----采用Direct(低级API)
//object SparkStreamingKafka_Direct {
// def main(args: Array[String]): Unit = {
// //1、创建sparkConf
//val sparkConf: SparkConf = new SparkConf()
//.setAppName("SparkStreamingKafka_Direct")
//.setMaster("local[2]")
////2、创建sparkContext
//val sc = new SparkContext(sparkConf)
//sc.setLogLevel("WARN")
////3、创建StreamingContext
// val ssc = new StreamingContext(sc,Seconds(5))
////4、配置kafka相关参数
// val kafkaParams=Map("metadata.broker.list"->"localhost:9092","group.id"->"Kafka_Direct")
// //5、定义topic
// val topics=Set("test3")
//
////6、通过 KafkaUtils.createDirectStream接受kafka数据，这里采用是kafka低级api偏移量不受zk管理
//
//val dstream: InputDStream[(String, String)] =KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
//
// //7、获取kafka中topic中的数据
//val topicData: DStream[String] = dstream.map(_._2)
// //8、切分每一行,每个单词计为1
//val wordAndOne: DStream[(String, Int)] = topicData.flatMap(_.split(" ")).map((_,1))
////9、相同单词出现的次数累加
//val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
////10、打印输出
// result.print()
////开启计算
// ssc.start()
// ssc.awaitTermination()
// }
//}
