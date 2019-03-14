//package xhb.SparkCommerce
//import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.sql.types.StringType
//import org.apache.log4j.Logger
//import org.apache.log4j.Level
//object SplitColTest {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder()
//      .appName(this.getClass.getSimpleName)
//      .master("local")
//      .getOrCreate()
//
//    //从内存中创建DataFrame
//    import spark.implicits._
//    val df = Seq("Ming,20,15552211521", "hong,19,13287994007", "zhi,21,15552211523").toDF("value")
//    df.show()
//
//    /**
//      * +-------------------+
//      * |              value|
//      * +-------------------+
//      * |Ming,20,15552211521|
//      * |hong,19,13287994007|
//      * | zhi,21,15552211523|
//      * +-------------------+
//      */
//
//    import org.apache.spark.sql.functions._
//    //方法1： 使用内置函数split，然后遍历添加列
//    val separator = ","
//    lazy val first = df.first()
//
//    val numAttrs = first.toString().split(separator).length
//    val attrs = Array.tabulate(numAttrs)(n => "col_" + n)
//    //按指定分隔符拆分value列，生成splitCols列
//    var newDF = df.withColumn("splitCols", split($"value", separator))
//    attrs.zipWithIndex.foreach(x => {
//      newDF = newDF.withColumn(x._1, $"splitCols".getItem(x._2))
//    })
//    newDF.show()
//
//    /**
//      * +-------------------+--------------------+-----+-----+-----------+
//      * |              value|           splitCols|col_0|col_1|      col_2|
//      * +-------------------+--------------------+-----+-----+-----------+
//      * |Ming,20,15552211521|[Ming, 20, 155522...| Ming|   20|15552211521|
//      * |hong,19,13287994007|[hong, 19, 132879...| hong|   19|13287994007|
//      * | zhi,21,15552211523|[zhi, 21, 1555221...|  zhi|   21|15552211523|
//      * +-------------------+--------------------+-----+-----+-----------+
//      */
//
//    //方法2：使用udf函数创建多列，然后合并
//    val attributes: Array[Attribute] = {
//      val numAttrs = first.toString().split(separator).length
//      //生成attributes
//      Array.tabulate(numAttrs)(i => NumericAttribute.defaultAttr.withName("value" + "_" + i))
//    }
//    //创建多列数据
//    val fieldCols = attributes.zipWithIndex.map(x => {
//      val assembleFunc = udf {
//        str: String =>
//          str.split(separator)(x._2)
//      }
//      assembleFunc(df("value").cast(StringType)).as(x._1.name.get, x._1.toMetadata())
//    })
//    //合并数据
//    df.select(col("*") +: fieldCols: _*).show()
//
//    /**
//      * +-------------------+-------+-------+-----------+
//      * |              value|value_0|value_1|    value_2|
//      * +-------------------+-------+-------+-----------+
//      * |Ming,20,15552211521|   Ming|     20|15552211521|
//      * |hong,19,13287994007|   hong|     19|13287994007|
//      * | zhi,21,15552211523|    zhi|     21|15552211523|
//      * +-------------------+-------+-------+-----------+
//      */
//  }
//}