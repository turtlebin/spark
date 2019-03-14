package StructureStreamingTest

import java.sql.Timestamp


object withWaterMarking extends App{
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder
  .master("local")
  .appName("StructuredNetworkWordCount")
  .getOrCreate()
  
import spark.implicits._
case class mark(timestamp:Timestamp,word:String)
val lines = spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", 9999)
  .load()
  val test=lines.as[mark].withWatermark("timestamp", "1 minutes").as[String].flatMap(_.split(" ")).groupBy("value").count()
  val query = test.writeStream
  .outputMode("append")
  .format("console")
  .start()

query.awaitTermination()
}