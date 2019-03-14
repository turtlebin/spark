package xhb.sparktest

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds

object sparkStreaming extends App {
  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(1))
//  val lines = ssc.socketTextStream("master",7777 )
  val lines=ssc.textFileStream("/home/xhb/local/streamTest")
  val words = lines.flatMap(_.split(" "))
  val pairs = words.map(word => (word, 1))
  val wordCounts = pairs.reduceByKey(_ + _)
  wordCounts.print()
  ssc.start() // 开始计算
  ssc.awaitTermination() // 等待计算被中断
}