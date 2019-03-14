package xhb.sparkStream

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds

object customStreamTest extends App {
  val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(1))
  val receiver=ssc.receiverStream(new CustomReceiver("master", 8888, false, 0))
  receiver.print()
  ssc.start()             // 开始计算
ssc.awaitTermination()  // 等待计算被中断
}