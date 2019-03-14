package joinBasedOnSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType

object sqlJoinBasedOnHive extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .enableHiveSupport()
    .master("local")
    .getOrCreate()
  import spark.implicits._
  import spark.sql
  sql("drop table if exists flight_1")
  sql("create table flight_1(id int,year string,month string,dayOfMonth string,dayOfWeek string) row format delimited fields terminated by ',' stored as textfile")
  //导入csv文件到hive必须要指明分隔符
  spark.sparkContext.parallelize(0 to 5).collect().
    foreach(x => sql(s"LOAD DATA LOCAL INPATH '/home/xhb/local/data/flight_1.csv/part-0000" + x +
      "-c0789e78-4025-4e00-8644-348574ee8974-c000.csv' into table flight_1"))
  //不能用通配符的方式去批量导入文件到hive中，因此只能自己写函数循环导入数据到hive，其他的spark自带的方法如textFile，load方法则可以，
  val flight_1DF=sql("select * from flight_1")
  flight_1DF.show()

  sql("drop table if exists flight_2")
  sql("create table flight_2(id int,DepTIme string,CRSDepTime string,ArrTime string,CRSArrTime string) row format delimited fields terminated by ',' stored as textfile")
  spark.sparkContext.parallelize(0 to 5).collect().
    foreach(x => sql(s"LOAD DATA LOCAL INPATH '/home/xhb/local/data/flight_2.csv/part-0000" + x +
      "-2c89d803-393a-4e85-9dfb-33fb9e621789-c000.csv' into table flight_2"))
  val flight_2DF = sql("select * from flight_2")
  flight_2DF.show()

  val joinResult = sql("select * from flight_1 left join flight_2 on flight_1.id=flight_2.id order by flight_1.id limit 5")
  joinResult.show()

}