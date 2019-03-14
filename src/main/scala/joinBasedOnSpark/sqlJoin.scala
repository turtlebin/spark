package joinBasedOnSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType

object sqlJoin extends App{
   val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .master("local")
  .getOrCreate()
  import spark.implicits._
  
  val flight_1=spark.sparkContext.textFile("/home/xhb/local/data/flight_1.csv/part*.csv")
  .map(x=>x.split(",")).map(attributes=>Row(attributes(0).toInt,attributes(1),attributes(2),attributes(3),attributes(4)))
  
  val schema=StructType(Array(
      StructField("id",IntegerType),
      StructField("Year",StringType),
      StructField("Month",StringType),
      StructField("DayOfMonth",StringType),
      StructField("DayOfWeek",StringType)
  ))
  val flight_1DF=spark.createDataFrame(flight_1, schema)
  flight_1DF.show()
  
  flight_1DF.createOrReplaceTempView("flight_1")
  
  val flight_2=spark.sparkContext.textFile("/home/xhb/local/data/flight_2.csv/part*.csv")
  .map(x=>x.split(",")).map(attributes=>Row(attributes(0).toInt,attributes(1),attributes(2),attributes(3),attributes(4)))
  
  val schema2=StructType(Array(
      StructField("id2",IntegerType),
      StructField("DepTime",StringType),
      StructField("CRSDepTime",StringType),
      StructField("ArrTime",StringType),
      StructField("CRSArrTime",StringType)
  ))
  val flight_2DF=spark.createDataFrame(flight_2, schema2)
  flight_2DF.show()
  
  flight_2DF.createOrReplaceTempView("flight_2")
  
  val joinResult2=spark.sql("select * from flight_1 left join flight_2 on flight_1.id=flight_2.id2 order by flight_1.id limit 5" )
  
  joinResult2.show()
  
//  val joinResult=flight_1DF.join(flight_2DF, $"id"===$"id2").orderBy("id").take(10)
//  joinResult.show()
//  println(joinResult.count())
}