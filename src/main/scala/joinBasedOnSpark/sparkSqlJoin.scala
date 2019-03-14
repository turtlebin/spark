package joinBasedOnSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType


object sparkSqlJoin extends App {
  val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .master("local")
  .getOrCreate()
  import spark.implicits._
  
  val flightDF=spark.read.format("csv")
  .option("sep", ",")
  .option("inferSchema", "true")
  .option("header", "true")
  .load("/home/xhb/local/data/2008.csv")
  
  flightDF.show()
  
  val schema: StructType = flightDF.schema.add(StructField("id", LongType))
  val dfRDD: RDD[(Row, Long)] = flightDF.rdd.zipWithIndex()
  val rowRDD:RDD[Row]=dfRDD.map(row=>Row.merge(row._1,Row(row._2)))
  val flightDF2 = spark.createDataFrame(rowRDD, schema)
  
  flightDF2.show()
  flightDF2.printSchema()
  val flight_1=flightDF2.select("id","Year","Month","DayOfMonth","DayOfWeek").write.format("csv").save("/home/xhb/local/data/flight_1.csv")
  val flight_2=flightDF2.select("id","DepTime","CRSDepTime","ArrTime","CRSArrTime").write.format("csv").save("/home/xhb/local/data/flight_2.csv")
  val flight_3=flightDF2.select("id","UniqueCarrier","FlightNum","TailNum").write.format("csv").save("/home/xhb/local/data/flight_3.csv")

}