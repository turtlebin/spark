package sparksql
import org.apache.spark.sql.SparkSession

object sqlUDFTest extends App {
val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("master", "local")
  .master("local")
  .getOrCreate()
import spark.implicits._
spark.udf.register("myAverage", MyAverage)
//读取工程
val df =spark.read.json("/home/xhb/local/spark-2.3.0-bin-hadoop2.7/examples/src/main/resources/employees.json")
df.createOrReplaceTempView("employees")
df.show()
val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
result.show()
}