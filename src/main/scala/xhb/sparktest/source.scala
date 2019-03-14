package xhb.sparktest

import org.apache.spark.sql.SparkSession

object source extends App{
   val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("master", "local")
  .master("local")
  .getOrCreate()
  import spark.implicits._
  val peopleDF = spark.read.json("/home/xhb/local/testtweet.json")
  peopleDF.write.parquet("people.parquet")

}