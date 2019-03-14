package sparksql

import org.apache.spark.sql.SparkSession

object sqlTest extends App{

val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("master", "local")
  .master("local")
  .getOrCreate()
import spark.implicits._
//val df = spark.read.json("people.json")//json作为数据源创建df
//df.printSchema()
//df.show()
//df.select("name").show
//df.select($"age">21).show()
//df.select($"name", $"age" + 1).show()

//df.createOrReplaceTempView("people")
//val sqlDF = spark.sql("SELECT * FROM people")
//sqlDF.show()


case class Person(name: String, age: Long)

// Encoders are created for case classes
val caseClassDS = Seq(Person("Andy", 32)).toDS()
caseClassDS.show()
// +----+---+
// |name|age|
// +----+---+
// |Andy| 32|
// +----+---+

// Encoders for most common types are automatically provided by importing spark.implicits._
val primitiveDS = Seq(1, 2, 3).toDS()
primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

// DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
val path = "people.json"
val peopleDS = spark.read.json(path).as[Person]
peopleDS.show()


// For implicit conversions like converting RDDs to DataFrames
}