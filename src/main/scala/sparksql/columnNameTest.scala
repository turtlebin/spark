package sparksql

import org.apache.spark.sql.SparkSession

object columnNameTest extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("master", "local")
    .master("local")
    .getOrCreate()
  import spark.implicits._
  val s="ni hao a"
  val s2="wo hen hao a"
  val r=spark.sparkContext.parallelize(Seq(s,s2)).toDF()
  val ds=r.as[String].map(_.split(" "))
  ds.printSchema()
  ds.select($"value".getItem(0)).show()
  ds.select($"value".getItem(3)).show()

  val df=ds.toDF()
  
}