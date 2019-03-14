package xhb.sparktest

import org.apache.spark.sql.SparkSession


object hiveTest extends App{
val spark = SparkSession
  .builder()
  .appName("Spark Hive Example")
  .master("local")
  .enableHiveSupport()
  .getOrCreate()
import spark.implicits._
import spark.sql
//println(spark.conf.get("spark.sql.warehouse.dir"))
//spark.sql("show tables").show()
//println(spark.conf.get("spark.sql.warehouse.dir"))
sql("select * from hive_1.student").show()
//eclipse中要使用hive on spark，必须先在项目中导入hive-site.xml,然后导入mysql的jar包，缺少前者将无法定位hive数据库位置，缺少后者将无法执行sql语句
//这与在命令行中执行的情况非常不一样，命令行中只需要指定driver-class-path为myslqjar包位置即可，另外秩序把hive-site.xml放在spark/conf目录下即可
}