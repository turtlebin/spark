package xhb.SparkCommerce

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.log4j.{Level,Logger}
import scala.collection.immutable.HashSet

object Movie_Users_Analyzer {
  
  def main(args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl="local[4]"
    var dataPath="/home/xhb/下载/ml-1m/"
    if(args.length>0){
      masterUrl=args(0)
    }else if(args.length>1){
      dataPath=args(1)
    }
    
    val sc=new SparkContext(new SparkConf()
        .setMaster(masterUrl)
        .setAppName("Movie_Users_Analyzer"))
    val usersRDD=sc.textFile(dataPath+"users.dat")
    val moviesRDD=sc.textFile(dataPath+"movies.dat")
    val occupationsRDD=sc.textFile(dataPath+"occupations.txt")
    val ratingsRDD=sc.textFile(dataPath+"ratings.dat")
    
    val usersBasic=usersRDD.map(_.split("::")).map(user=>(user(3),(user(0),user(1),user(2))))
    val occupations=occupationsRDD.map(_.split("::")).map(job=>(job(0),job(1)))
    val userInformation=usersBasic.join(occupations)
    userInformation.cache()
    
    val targetMovie=ratingsRDD.map(_.split("::")).map(x=>(x(0),x(1))).filter(_._2.equals("1193"))
    val targetUsers=userInformation.map(x=>(x._2._1._1,x._2))
    val userInformationForSpecificMovie=targetMovie.join(targetUsers)
    
    //ratings:userID:movieID:rating:Timestamp
    val ratings=ratingsRDD.map(_.split("::")).map(x=>(x(0),x(1),x(2))).cache()
    ratings.map(x=>(x._2,(x._3.toDouble,1))).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
    .map(x=>(x._2._1/x._2._2.toDouble,x._1))
    .sortByKey(false)
    .take(10)
    .foreach(println)
    
    println("所有电影中粉丝或者观看人数最多的电影:")
    ratings.map(x=>(x._2,1)).reduceByKey(_+_).map(x=>(x._2,x._1))
    .sortByKey(false).take(10).foreach(println)
    //user:userID::Gender::Age::OccupationID::zip-code
    //movies:movieID::title::genres
    //ratings:userID::movieID::rating::timestamp
    val targetQQUsers=usersRDD.map(_.split("::")).map(x=>(x(0),x(2))).filter(_._2.equals("18"))
    val targetTaobaoUsers=usersRDD.map(_.split("::")).map(x=>(x(0),x(2))).filter(_._2.equals("25"))
    val targetQQUserSet=HashSet()++targetQQUsers.map(_._1).collect()
    val targetTaobaoUserSet=HashSet()++targetTaobaoUsers.map(_._1).collect()
    val targetQQUserBroad=sc.broadcast(targetQQUserSet)
    val targetTaobaoUserBroad=sc.broadcast(targetTaobaoUserSet)
    
    val movieID2Name=moviesRDD.map(_.split("::")).map(x=>(x(0),x(1))).collect().toMap
    println("所有电影中QQ或者微信核心用户最喜欢电影TOPN分析")
    //(userID,movieID)
    ratingsRDD.map(_.split("::"))
    .map(x=>(x(0),x(1)))
    .filter(x=>targetQQUserBroad.value.contains(x._1))
    .map(x=>(x._2,1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1)).take(10)
    .map(x=>(movieID2Name.getOrElse(x._1,null),x._2)).foreach(println)
    
    println("对电影评分数据以Timestamp和Rating两个唯独进行二次降序排列")
    val pairWithSortKey=ratingsRDD.map{line=>{
      val splited=line.split("::")
      (new SecondarySortKey(splited(3).toDouble,splited(2).toDouble),line)
    }}
    
    val sorted=pairWithSortKey.sortByKey(false)
    val sortedResult=sorted.map(_._2)
    sortedResult.take(10).foreach(println)
    
    while(true){}//这样可以一直运行，使webUI不关闭
    sc.stop()
  }
}