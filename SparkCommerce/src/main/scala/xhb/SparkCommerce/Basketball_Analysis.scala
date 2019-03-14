package xhb.SparkCommerce

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.storage.StorageLevel
object Basketball_Analysis {
  def main(args: Array[String]) { //没有=号的函数没有返回值
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[8]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("NBAPlayer_Analyzer_DataSet")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val data_path = "/home/xhb/local/data/NBA"
    val data_Tmp = "/home/xhb/local/data/NBA_Temp"

    FileSystem.get(new Configuration()).delete(new Path(data_Tmp), true) //hadoop中的Api,如果文件路径存在，则删除对应文件夹
    for (year <- 2008 to 2017) {
      val statsPerYear = sc.textFile(s"${data_path}/sports_${year}*").repartition(sc.defaultParallelism)
      statsPerYear.filter(_.contains(",")).map(line => (year, line)).saveAsTextFile(s"${data_Tmp}/NBAStatsPerYear/${year}")
    }

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._
    import org.apache.spark.util.StatCounter
    import scala.collection.mutable.ListBuffer

    def statNormalize(stat: Double, max: Double, min: Double) = {
      val newmax = math.max(math.abs(max), math.abs(min))
      stat / newmax
    }
    case class BballData(val year: Int, name: String, position: String, age: Int, team: String, gp: Int, gs: Int, mp: Double, stats: Array[Double], statsZ: Array[Double] = Array[Double](), valueZ: Double = 0, statsN: Array[Double] = Array[Double](), valueN: Double = 0, experience: Double = 0)

    //parse a stat line into a BBallDataZ object
    def bbParse(input: String, bStats: scala.collection.Map[String, Double] = Map.empty, zStats: scala.collection.Map[String, Double] = Map.empty) = {
      val line = input.replace(",,", ",0,")
      val pieces = line.substring(1, line.length - 1).split(",")
      val year = pieces(0).toInt
      val name = pieces(2)
      val position = pieces(3)
      val age = pieces(4).toInt
      val team = pieces(5)
      val gp = pieces(6).toInt
      val gs = pieces(7).toInt
      val mp = pieces(8).toDouble
      val stats = pieces.slice(9, 31).map(x => x.toDouble) //抽取数组中元素
      var statsZ: Array[Double] = Array.empty
      var valueZ: Double = Double.NaN //scala中设置double初始值的一种方式，类似的由Double.POSITIVE_INFINITY
      var statsN: Array[Double] = Array.empty
      var valueN: Double = Double.NaN

      if (!bStats.isEmpty) {
        val fg = (stats(2) - bStats.apply(year.toString + "_FG%_avg")) * stats(1)
        val tp = (stats(3) - bStats.apply(year.toString + "_3P_avg")) / bStats.apply(year.toString + "_3P_stdev")
        val ft = (stats(12) - bStats.apply(year.toString + "_FT%_avg")) * stats(11)
        val trb = (stats(15) - bStats.apply(year.toString + "_TRB_avg")) / bStats.apply(year.toString + "_TRB_stdev")
        val ast = (stats(16) - bStats.apply(year.toString + "_AST_avg")) / bStats.apply(year.toString + "_AST_stdev")
        val stl = (stats(17) - bStats.apply(year.toString + "_STL_avg")) / bStats.apply(year.toString + "_STL_stdev")
        val blk = (stats(18) - bStats.apply(year.toString + "_BLK_avg")) / bStats.apply(year.toString + "_BLK_stdev")
        val tov = (stats(19) - bStats.apply(year.toString + "_TOV_avg")) / bStats.apply(year.toString + "_TOV_stdev") * (-1)
        val pts = (stats(21) - bStats.apply(year.toString + "_PTS_avg")) / bStats.apply(year.toString + "_PTS_stdev")
        statsZ = Array(fg, ft, tp, trb, ast, stl, blk, tov, pts)
        valueZ = statsZ.reduce(_ + _)

        if (!zStats.isEmpty) {
          val zfg = (fg - zStats.apply(year.toString + "_FG_avg")) / zStats.apply(year.toString + "_FG_stdev")
          val zft = (ft - zStats.apply(year.toString + "_FT_avg")) / zStats.apply(year.toString + "_FT_stdev")
          val fgN = statNormalize(zfg, (zStats.apply(year.toString + "_FG_max") - zStats.apply(year.toString + "_FG_avg"))
            / zStats.apply(year.toString + "_FG_stdev"), (zStats.apply(year.toString + "_FG_min")
            - zStats.apply(year.toString + "_FG_avg")) / zStats.apply(year.toString + "_FG_stdev"))
          val ftN = statNormalize(zft, (zStats.apply(year.toString + "_FT_max") - zStats.apply(year.toString + "_FT_avg"))
            / zStats.apply(year.toString + "_FT_stdev"), (zStats.apply(year.toString + "_FT_min")
            - zStats.apply(year.toString + "_FT_avg")) / zStats.apply(year.toString + "_FT_stdev"))
          val tpN = statNormalize(tp, zStats.apply(year.toString + "_3P_max"), zStats.apply(year.toString + "_3P_min"))
          val trbN = statNormalize(trb, zStats.apply(year.toString + "_TRB_max"), zStats.apply(year.toString + "_TRB_min"))
          val astN = statNormalize(ast, zStats.apply(year.toString + "_AST_max"), zStats.apply(year.toString + "_AST_min"))
          val stlN = statNormalize(stl, zStats.apply(year.toString + "_STL_max"), zStats.apply(year.toString + "_STL_min"))
          val blkN = statNormalize(blk, zStats.apply(year.toString + "_BLK_max"), zStats.apply(year.toString + "_BLK_min"))
          val tovN = statNormalize(tov, zStats.apply(year.toString + "_TOV_max"), zStats.apply(year.toString + "_TOV_min"))
          val ptsN = statNormalize(pts, zStats.apply(year.toString + "_PTS_max"), zStats.apply(year.toString + "_PTS_min"))
          statsZ = Array(zfg, zft, tp, trb, ast, stl, blk, tov, pts)
          valueZ = statsZ.reduce(_ + _)
          statsN = Array(fgN, ftN, tpN, trbN, astN, stlN, blkN, tovN, ptsN)
          valueN = statsN.reduce(_ + _)
        }
      }
      BballData(year, name, position, age, team, gp, gs, mp, stats, statsZ, valueZ, statsN, valueN)
    }

    class BballStatCounter extends Serializable {
      val stats: StatCounter = new StatCounter()
      var missing: Long = 0

      def add(x: Double): BballStatCounter = {
        if (x.isNaN) {
          missing += 1
        } else {
          stats.merge(x)
        }
        this
      }

      def merge(other: BballStatCounter): BballStatCounter = {
        stats.merge(other.stats)
        missing += other.missing
        this
      }

      def printStats(delim: String): String = {
        stats.count + delim + stats.mean + delim + stats.stdev + delim + stats.max + delim + stats.min
      }

      override def toString: String = {
        "stats: " + stats.toString + " NaN: " + missing
      }
    }

    object BballStatCounter extends Serializable {
      def apply(x: Double) = new BballStatCounter().add(x)
      //在这里使用了Scala语言的一个编程技巧，借助于apply工厂方法，在构造该对象的时候就可以执行出结果
    }

    //process raw data into zScores and nScores
    def processStats(stats0: org.apache.spark.rdd.RDD[String], txtStat: Array[String],
      bStats: scala.collection.Map[String, Double] = Map.empty,
      zStats: scala.collection.Map[String, Double] = Map.empty) = {
      //parse stats
      val stats1 = stats0.map(x => bbParse(x, bStats, zStats))

      //group by year
      val stats2 = {
        if (bStats.isEmpty) {
          stats1.keyBy(x => x.year).map(x => (x._1, x._2.stats)).groupByKey()
        } else {
          stats1.keyBy(x => x.year).map(x => (x._1, x._2.statsZ)).groupByKey()
        }
      }

      //map each stat to StatCounter
      val stats3 = stats2.map { case (x, y) => (x, y.map(a => a.map(b => BballStatCounter(b)))) } //此处只是初始化StatCounter对象

      //merge all stats together
      val stats4 = stats3.map { case (x, y) => (x, y.reduce((a, b) => a.zip(b).map { case (c, d) => c.merge(d) })) }
      //stats4为(year,Array[BballStatCounter])
      //combine stats with label and pull label out
      val stats5 = stats4.map { case (x, y) => (x, txtStat.zip(y)) }.map {
        x =>
          (x._2.map {
            case (y, z) => (x._1, y, z)
          })
      }
      //stats5为(year,统计量名称,BballStatCounter)
      //separate each stat onto its own line and print out the Stats to a String
      val stats6 = stats5.flatMap(x => x.map(y => (y._1, y._2, y._3.printStats(","))))

      //turn stat tuple into key-value pairs with corresponding agg stat
      val stats7 = stats6.flatMap {
        case (a, b, c) => {
          val pieces = c.split(",")
          val count = pieces(0)
          val mean = pieces(1)
          val stdev = pieces(2)
          val max = pieces(3)
          val min = pieces(4)
          Array(
            (a + "_" + b + "_" + "count", count.toDouble),
            (a + "_" + b + "_" + "avg", mean.toDouble),
            (a + "_" + b + "_" + "stdev", stdev.toDouble),
            (a + "_" + b + "_" + "max", max.toDouble),
            (a + "_" + b + "_" + "min", min.toDouble))
        }
      }
      stats7
    }
    //process stats for age or experience
    def processStatsAgeOrExperience(stats0: org.apache.spark.rdd.RDD[(Int, Array[Double])], label: String) = {
      //group elements by age
      val stats1 = stats0.groupByKey()

      //turn values into StatCounter objects
      val stats2 = stats1.map { case (x, y) => (x, y.map(z => z.map(a => BballStatCounter(a)))) }

      //Reduce rows by merging StatCounter objects
      val stats3 = stats2.map { case (x, y) => (x, y.reduce((a, b) => a.zip(b).map { case (c, d) => c.merge(d) })) }

      //turn data into RDD[Row] object for dataframe
      val stats4 = stats3.map(x => Array(
        Array(x._1.toDouble),
        x._2.flatMap(y => y.printStats(",").split(",")).map(y => y.toDouble)).flatMap(y => y))
        .map(x =>
          Row(x(0).toInt, x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8),
            x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20)))

      //create schema for age table
      val schema = StructType(
        StructField(label, IntegerType, true) ::
          StructField("valueZ_count", DoubleType, true) ::
          StructField("valueZ_mean", DoubleType, true) ::
          StructField("valueZ_stdev", DoubleType, true) ::
          StructField("valueZ_max", DoubleType, true) ::
          StructField("valueZ_min", DoubleType, true) ::
          StructField("valueN_count", DoubleType, true) ::
          StructField("valueN_mean", DoubleType, true) ::
          StructField("valueN_stdev", DoubleType, true) ::
          StructField("valueN_max", DoubleType, true) ::
          StructField("valueN_min", DoubleType, true) ::
          StructField("deltaZ_count", DoubleType, true) ::
          StructField("deltaZ_mean", DoubleType, true) ::
          StructField("deltaZ_stdev", DoubleType, true) ::
          StructField("deltaZ_max", DoubleType, true) ::
          StructField("deltaZ_min", DoubleType, true) ::
          StructField("deltaN_count", DoubleType, true) ::
          StructField("deltaN_mean", DoubleType, true) ::
          StructField("deltaN_stdev", DoubleType, true) ::
          StructField("deltaN_max", DoubleType, true) ::
          StructField("deltaN_min", DoubleType, true) :: Nil)

      //create data frame
      spark.createDataFrame(stats4, schema)
    }

    val NBAStats = sc.textFile(s"${data_Tmp}/NBAStatsPerYear/*/*").repartition(sc.defaultParallelism)
    val filteredData = NBAStats.filter(!_.contains("FG%")).filter(line => line.contains(",")).map(line => line.replace(",,", ",0,").replace("*", ""))
    filteredData.collect().take(10).foreach(println)
    filteredData.persist(StorageLevel.MEMORY_AND_DISK)
    //
    val txtStat: Array[String] = Array("FG", "FGA", "FG%", "3P", "3PA", "3P%", "2P", "2PA", "2P%", "eFG%", "FT", "FTA", "FT%", "ORB", "DRB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS")
    println("NBA球员数据统计维度：")
    txtStat.foreach(println)
    val aggStats = processStats(filteredData, txtStat).collectAsMap
    println("NBA球员基础数据项aggStats MAP映射集：")
    aggStats.take(20).foreach { case (k, v) => println("(" + k + "," + v + ")") }
    val broadcastStats = sc.broadcast(aggStats)

    val txtStatZ = Array("FG", "FT", "3P", "TRB", "AST", "STL", "BLK", "TOV", "PTS")
    val zStats = processStats(filteredData, txtStatZ, broadcastStats.value).collectAsMap()
    println("NBA球员Z-Score标准分zStats Map映射集：")
    zStats.take(10).foreach { case (k, v) => println("(" + k + "," + v + ")") }
    val zBroadcastStats = sc.broadcast(zStats)

    val nStats = filteredData.map(x => bbParse(x, broadcastStats.value, zBroadcastStats.value))
    val nPlayer = nStats.map(x => {
      val nPlayerRow = Row.fromSeq(Array(x.name, x.year, x.age, x.position, x.team, x.gp, x.gs, x.mp) ++ x.stats ++ x.statsZ ++ Array(x.valueZ) ++ x.statsN ++ Array(x.valueN))
      nPlayerRow
    })

    val schemaN = StructType(
      StructField("name", StringType, true) :://1
        StructField("year", IntegerType, true) :://2
        StructField("age", IntegerType, true) :://3
        StructField("position", StringType, true) :://4
        StructField("team", StringType, true) :://5
        StructField("gp", IntegerType, true) ::
        StructField("gs", IntegerType, true) ::
        StructField("mp", DoubleType, true) ::
        StructField("FG", DoubleType, true) ::
        StructField("FGA", DoubleType, true) :://10
        StructField("FGP", DoubleType, true) ::
        StructField("3P", DoubleType, true) ::
        StructField("3PA", DoubleType, true) ::
        StructField("3PP", DoubleType, true) ::
        StructField("2P", DoubleType, true) ::
        StructField("2PA", DoubleType, true) ::
        StructField("2PP", DoubleType, true) ::
        StructField("eFG", DoubleType, true) ::
        StructField("FT", DoubleType, true) ::
        StructField("FTA", DoubleType, true) :://20
        StructField("FTP", DoubleType, true) ::
        StructField("ORB", DoubleType, true) ::
        StructField("DRB", DoubleType, true) ::
        StructField("TRB", DoubleType, true) ::
        StructField("AST", DoubleType, true) ::
        StructField("STL", DoubleType, true) ::
        StructField("BLK", DoubleType, true) ::
        StructField("TOV", DoubleType, true) ::
        StructField("PF", DoubleType, true) ::
        StructField("PTS", DoubleType, true) :://30
        StructField("zFG", DoubleType, true) :://31
        StructField("zFT", DoubleType, true) :://32
        StructField("z3P", DoubleType, true) :://33
        StructField("zTRB", DoubleType, true) :://34
        StructField("zAST", DoubleType, true) :://35
        StructField("zSTL", DoubleType, true) :://36
        StructField("zBLK", DoubleType, true) :://37
        StructField("zTOV", DoubleType, true) :://38
        StructField("zPTS", DoubleType, true) :://39
        StructField("zTOT", DoubleType, true) :://40
        StructField("nFG", DoubleType, true) :://41
        StructField("nFT", DoubleType, true) :://42
        StructField("n3P", DoubleType, true) :://43
        StructField("nTRB", DoubleType, true) :://44
        StructField("nAST", DoubleType, true) :://45
        StructField("nSTL", DoubleType, true) :://46
        StructField("nBLK", DoubleType, true) :://47
        StructField("nTOV", DoubleType, true) :://48
        StructField("nPTS", DoubleType, true) :://49
        StructField("nTOT", DoubleType, true) :: Nil)//50

    val dfPlayersT = spark.createDataFrame(nPlayer, schemaN)
    dfPlayersT.createOrReplaceTempView("tPlayers")

    val dfPlayers = spark.sql("select age-min_age as exp,tPlayers.* from tPlayers join" +
      " (select name,min(age)as min_age from tPlayers group by name) as t1" +
      " on tPlayers.name=t1.name order by tPlayers.name, exp ")

    println("计算exp and zdiff,ndiff")
    dfPlayers.show()
    dfPlayers.createOrReplaceTempView("Players")
    println("打印NBA球员的历年比赛记录")
    dfPlayers.rdd.map(x => (x.getString(1), x)).filter(_._1.contains("A.C. Green")).foreach(println)

    import spark.implicits._

    val pStats = dfPlayers.sort(dfPlayers("name"), dfPlayers("exp") asc).rdd.map { x => //注意这里的sort参数形式，也可以使用$"name".asc的形式，但这种需要导入sparkSession.implicits._
      (x.getString(1), (x.getDouble(50), x.getDouble(40), x.getInt(2), x.getInt(3),
        Array(x.getDouble(31), x.getDouble(32), x.getDouble(33), x.getDouble(34), x.getDouble(35),
          x.getDouble(36), x.getDouble(37), x.getDouble(38), x.getDouble(39)), x.getInt(0)))
    }.groupByKey()
    pStats.cache()
    println("根据NBA球员名字分组:")
    pStats.take(15).foreach(x => {
      val myx2 = x._2
      println(s"按NBA球员:${x._1}进行分组，组中元素个数为:${myx2.size}")
      for (i <- 1 to myx2.size) {
        val myx2size = myx2.toArray
        val mynext = myx2size(i - 1)
      }
    })
    //pStats为（name,Iterator((name,(nTOT,zTOT,year,age,Array(zFG,zFT,z3P,zTRB,zAST,zSTL,zBLK,zTOV,zPTS),exp)))）
    val excludeNames = dfPlayers.filter(dfPlayers("year") === 1980).select("name").map(x => x.mkString).collect().mkString(",")
    //有时候用mkString比用toString更方便一点
    val pStats1 = pStats.map {
      case (name, stats) =>
        var last = 0
        var deltaZ = 0.0
        var deltaN = 0.0
        var valueZ = 0.0
        var valueN = 0.0
        var exp = 0
        val aList = ListBuffer[(Int, Array[Double])]()
        val eList = ListBuffer[(Int, Array[Double])]()
        stats.foreach(z => {
          if (last > 0) {
            deltaN = z._1 - valueN
            deltaZ = z._2 - valueZ
          } else {
            deltaN = Double.NaN
            deltaZ = Double.NaN
          }
          valueN = z._1
          valueZ = z._2
          last = z._4
          aList+=((last,Array(valueZ,valueN,deltaZ,deltaN)))//age
          if(!excludeNames.contains(name)){
            exp=z._6
            eList+=((exp,Array(valueZ,valueN,deltaZ,deltaN)))//exp
          }
        })
        (aList,eList)
    }
    pStats1.cache()
    println("按NBA球员的年龄及经验值进行统计:")
    pStats1.take(10).foreach(x=>{
      for(i<-1 to x._1.size){
        println(s"年龄:${x._1(i-1)._1},${x._1(i-1)._2.mkString("||")} 经验: ${x._2(i-1)._1},${x._2(i-1)._2.mkString("||")}")
      }
    })
  }
}