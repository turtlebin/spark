package StructureStreamingTest

import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

case class test1(id:String,year:String,day:String,week:String,month:String)
case class test2(ano_id:String,ano_year:String,ano_day:String,ano_week:String,ano_month:String)
case class test3(id:String,year:String)
object kafkaTest extends App {
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("spark://master:7077")
//    .master("local[4]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()
  import spark.implicits._
  
  val df1 = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "master:9092,slave2:9092,slave3:9092")
    .option("startingOffsets", "earliest")
    //                .option("security.protocol","SASL_PLAINTEXT")
    //                .option("sasl.mechanism","PLAIN")
    .option("subscribe", "minJoin_1")
    .load()
  val fields1 = StructField("id", IntegerType, true) :: StructField("age", IntegerType, true) :: StructField("height", IntegerType, true) :: Nil
  val type1=StructType(fields1)
//  val test=df1.as[String].map(_.split("_"))
  
  val d1 = df1.withWatermark("timestamp", "1 hours")
    .selectExpr("CAST(value AS String)")
    .select(split($"value",",").getItem(0).as("id"),split($"value",",").getItem(1).as("year"),split($"value",",").getItem(2).as("day"),split($"value",",").getItem(3).as("week"),split($"value",",").getItem(4).as("month"))
    .as[test1]
    //    .selectExpr($"value".toString().split(",")(0) ,$"value".toString().split(",")(1),$"value".toString().split(",")(2),$"value".toString().split(",")(3),$"value".toString().split(",")(4))
//    .as[test1].as("v")
//    .selectExpr("v.id","v.year","v.week","v.day","v.month")
//    .rdd
//    .toDS().as("v").selectExpr("v.id","v.year","v.day","v.week","v.month")
//    .select(from_json(col("value"), type1).as("v"))
//    .selectExpr("v.id", "v.age", "v.height");
  val df2 = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "master:9092,slave2:9092,slave3:9092")
    .option("startingOffsets", "earliest")
    //                .option("security.protocol","SASL_PLAINTEXT")
    //                .option("sasl.mechanism","PLAIN")
    .option("subscribe", "minJoin_2")
    .load()
//
//  val fields2 = StructField("yh2_id", IntegerType, true) :: StructField("yh2_age", IntegerType, true) :: StructField("yh2_height", IntegerType, true) :: Nil
//  val type2=StructType(fields2)
  val d2 = df2.withWatermark("timestamp", "1 hours")
    .selectExpr("CAST(value AS STRING)")
    .select(split($"value",",").getItem(0).as("ano_id"),split($"value",",").getItem(1).as("ano_year"),split($"value",",").getItem(2).as("ano_day"),split($"value",",").getItem(3).as("ano_week"),split($"value",",").getItem(4).as("ano_month"))
    .as[test2]
//    .select(split($"value",","))
//    .as[test2].as("v")
//    .selectExpr("v.ano_id","v.year","v.week","v.day","v.month")
////    .select(from_json(col("value"), type2).as("v"))
////    .selectExpr("v.yh2_id","v.yh2_age","v.yh2_height");
//  val query = d1.join(d2,expr("id = ano_id")).writeStream.format("console").outputMode("append").start()
  d1.join(d2,expr("id = ano_id")).printSchema()
   val query = d1.join(d2,expr("id = ano_id"))
   .select($"id".alias("key"),concat($"id",$"year",$"day",$"ano_id",$"ano_month",$"ano_week").alias("value"))//导出到kafka必须要有value列
   .writeStream
//   .format("console")
   .format("kafka")
   .option("kafka.bootstrap.servers", "master:9092,slave2:9092,slave3:9092")
   .option("topic", "update8")
   .option("checkpointLocation","hdfs://master:9000//checkpoint")
   .start()
  
  query.awaitTermination()

//  val d1_test= d1.writeStream.format("console").outputMode("append").start()
//  d1_test.awaitTermination()

}