package xhb.SparkCommerce

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}

object SparkDataFrameTimeWindow {
  def main(args: Array[String]): Unit = {

    //设置日志等级
    Logger.getLogger("org").setLevel(Level.WARN)

    //spark环境
    val spark = SparkSession.builder().master("local[3]").appName(this.getClass.getSimpleName.replace("$","")).getOrCreate()
    import spark.implicits._

    //读取时序数据
    val data = spark.read.option("header","true").option("inferSchema","true").csv("/home/xhb/local/data/monitor.csv")
    //data.printSchema()

    /** 1) Tumbling window */
    /** 计算: 每10分钟的平均CPU、平均内存、平均磁盘，并保留两位小数 */
    data
      .filter(functions.year($"eventTime").between(2017,2018))
      .groupBy(functions.window($"eventTime","10 minute")) //Time Window
      .agg(functions.round(functions.avg($"cpu"),2).as("avgCpu"),functions.round(functions.avg($"memory"),2).as("avgMemory"),functions.round(functions.avg($"disk"),2).as("avgDisk"))
      .sort($"window.start").select($"window.start",$"window.end",$"avgCpu",$"avgMemory",$"avgDisk")
      .limit(5)
      .show(false)


    /** 2) Slide window */
    /** 计算：从第3分钟开始，每5分钟计算最近10分钟内的平均CPU、平均内存、平均磁盘，并保留两位小数 */
    data
      .filter(functions.year($"eventTime").between(2017,2018))
      .groupBy(functions.window($"eventTime","10 seconds","5 seconds","3 secondss")) //Time Window
      .agg(functions.round(functions.avg($"cpu"),2).as("avgCpu"),functions.round(functions.avg($"memory"),2).as("avgMemory"),functions.round(functions.avg($"disk"),2).as("avgDisk"))
      .sort($"window.start").select($"window.start",$"window.end",$"avgCpu",$"avgMemory",$"avgDisk")
      .limit(5)
      .show(false)
  }
}
