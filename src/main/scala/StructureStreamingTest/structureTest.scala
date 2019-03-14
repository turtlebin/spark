package StructureStreamingTest

object structureTest extends App{
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder
  .master("local")
  .appName("StructuredNetworkWordCount")
  .getOrCreate()
  
import spark.implicits._
val lines = spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", 9999)
  .load()

// Split the lines into words
val words = lines.as[String].flatMap(_.split(" "))

// Generate running word count
val wordCounts = words.groupBy("value").count()

val query = wordCounts.writeStream
  .outputMode("append")
  .format("console")
  .start()

query.awaitTermination()
}