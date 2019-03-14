package xhb.SparkCommerce

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.LocationStrategies
import scala.collection.mutable.ListBuffer
import java.sql.ResultSet
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.hive.HiveContext
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.shuffle.sort.PackedRecordPointer

object AdClickedStreamingStats {
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("blackList").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("/home/xhb/local/checkpoint")
    
    val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean))
    
    val topics = Set[String]("re2")
    
    val adClickedStreaming = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    val filteredadClickedStreaming = adClickedStreaming.transform(rdd => {
      val blackListNames = ListBuffer[String]()
      val jdbcWrapper = JDBCWrapper.getInstance()
      def queryCallBack(result: ResultSet): Unit = { //闭包方法
        while (result.next()) {
          result.getString(1)
          blackListNames += result.getString(1)
        }
      }
      jdbcWrapper.doQuery("select * from blacklisttable", null, queryCallBack) //通过查询，然后修改闭包中引用的局部变量，然后再进行下一步处理
      val blackListTuple = ListBuffer[(String, Boolean)]()
      for (name <- blackListNames) {
        val nameBoolean = (name, true)
        blackListTuple += nameBoolean
      }
      val blackListFromDB = blackListTuple
      val jsc: SparkContext = rdd.sparkContext
      val blackListRDD = jsc.parallelize(blackListFromDB)
      val rdd2Pair = rdd.map(t => { //这里是不一样的
        val userID = t.value().split("\t")(2)
        (userID, t)
      })
      val record=rdd2Pair.map(x=>(x._1,(x._2.key(),x._2.value())))
      val joined = record.leftOuterJoin(blackListRDD)
      val result = joined.filter(v1 => {
        val optional = v1._2._2
        if (optional.isDefined && optional.get) { //过滤掉blackList中有的名单
          false
        } else {
          true
        }
      }).map(_._2._1)
      result
    })

    filteredadClickedStreaming.print()

    val pairs = filteredadClickedStreaming.map(t => {
      val splited = t._2.split("\t")
      val timestamp = splited(0)
      val ip = splited(1)
      val userID = splited(2)
      val adID = splited(3)
      val province = splited(4)
      val city = splited(5)
      val clickedRecord = s"${timestamp}_${ip}_${userID}_${adID}_${province}_${city}"
      (clickedRecord, 1L)
    })

    val adClickedUsers = pairs.reduceByKey(_ + _) //统计

    val filteredClickInBatch = adClickedUsers.filter(v1 => {
      if (1 < v1._2) {
        //过滤掉一个batch内点击次数大于1的用户，这里可以添加一个逻辑，用于更新黑名单的数据表
        false
      } else {
        true
      }
    })
    filteredClickInBatch.print()

    filteredClickInBatch.foreachRDD(rdd => {
      if (rdd.isEmpty()) {}
      rdd.foreachPartition(partition => {
        val userAdClickedList = ListBuffer[UserAdClicked]()
        while (partition.hasNext) {
          val record = partition.next()
          val splited = record._1.split("_")
          val userClicked = new UserAdClicked()
          userClicked.timestamp = splited(0)
          userClicked.ip = splited(1)
          userClicked.userID = splited(2)
          userClicked.adID = splited(3)
          userClicked.province = splited(4)
          userClicked.city = splited(5)
          userAdClickedList += userClicked
        }
        val inserting = ListBuffer[UserAdClicked]()
        val updating = ListBuffer[UserAdClicked]()
        val jdbcWrapper = JDBCWrapper.getInstance()

        for (clicked <- userAdClickedList) {
          def clickedquerycallBack(result: ResultSet) = {
            if (result.next()) {
                val count = result.getLong(1)
                clicked.clickedCount =count+1//个人觉得这里需要+1吧？这里与书本很不同
                updating += clicked
            } else {
                clicked.clickedCount = 1L
                inserting += clicked
              }
          }
          jdbcWrapper.doQuery("select clickedCount from adclicked where timestamp= ? and userID= ? and adID= ? and province=? and city =? ", Array(
            clicked.timestamp,
            clicked.userID, clicked.adID,clicked.province,clicked.city), clickedquerycallBack)
        }
        val insertParametersList = ListBuffer[paramsList]()
        for (insertRecord <- inserting) {
          val paramsListTmp = new paramsList()
          paramsListTmp.params1 = insertRecord.timestamp
          paramsListTmp.params2 = insertRecord.ip
          paramsListTmp.params3 = insertRecord.userID
          paramsListTmp.params4 = insertRecord.adID
          paramsListTmp.params5 = insertRecord.province
          paramsListTmp.params6 = insertRecord.city
          paramsListTmp.params10_Long = insertRecord.clickedCount
          paramsListTmp.params_Type = "adclickedInsert"
          insertParametersList += paramsListTmp
        }
        jdbcWrapper.doBatch("insert into adclicked values(?,?,?,?,?,?,?)", insertParametersList)

        val updateParametersList = ListBuffer[paramsList]()
        for (updateRecord <- updating) {
          val paramsListTmp = new paramsList()
          paramsListTmp.params1 = updateRecord.timestamp
          paramsListTmp.params2 = updateRecord.ip
          paramsListTmp.params3 = updateRecord.userID
          paramsListTmp.params4 = updateRecord.adID
          paramsListTmp.params5 = updateRecord.province
          paramsListTmp.params6 = updateRecord.city
          paramsListTmp.params10_Long = updateRecord.clickedCount
          paramsListTmp.params_Type = "adclickedUpdate"
          updateParametersList += paramsListTmp
        }
        jdbcWrapper.doBatch("update adclicked set clickedCount= ? where timestamp= ? and ip= ? and userID= ? and adID= ? and province= ? "
          + " and city= ?", updateParametersList)
      })
    })
    
    val blackListBasedOnHistory=filteredClickInBatch.filter(v1=>{
      val splited=v1._1.split("_")
      val date=splited(0)
      val userID=splited(2)
      val adID=splited(3)
      val clickedCountTotalToday=81//此处进一步进行过滤，为了方便直接设置数值为81，而不再查询数据库
      if(clickedCountTotalToday>1){
        true
      }else{
        false
      }
    })
    
    val blackListuserIDtBasedOnHistory=blackListBasedOnHistory.map(_._1.split("_")(2))//userID
    val blackListUniqueuserIDtBasedOnHistory=blackListuserIDtBasedOnHistory.transform(_.distinct())
    
    blackListUniqueuserIDtBasedOnHistory.foreachRDD(rdd=>{
      rdd.foreachPartition(t=>{
        val blackList=ListBuffer[paramsList]()
        while(t.hasNext){
          val paramsListTmp=new paramsList()
          paramsListTmp.params1=t.next()
          paramsListTmp.params_Type="blacklisttableInsert"
          blackList+=paramsListTmp
        }
        val jdbcWrapper=JDBCWrapper.getInstance()
        jdbcWrapper.doBatch("insert into blacklisttable values (?)", blackList)
      })
    })
    
    val filteredadClickedStreamingmappair=filteredadClickedStreaming.map(t=>{
      val splited=t._2.split("\t")
      val timestamp=splited(0)
      val ip=splited(1)
      val userID=splited(2)
      val adID=splited(3)
      val province=splited(4)
      val city=splited(5)
      val clickedRecord=s"${timestamp}_${adID}_${province}_${city}"
      (clickedRecord,1L)
    })
    
    val updateFunc=(values:Seq[Long],state:Option[Long])=>{
      Some[Long](values.sum+state.getOrElse(0L))
    }
    
    val updateStateByKeyDStream=filteredadClickedStreamingmappair.updateStateByKey(updateFunc)
    
    updateStateByKeyDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val adClickedList=ListBuffer[AdClicked]()
        while(partition.hasNext){
          val record=partition.next()
          val splited=record._1.split("_")
          val adClicked=new AdClicked()
          adClicked.timestamp=splited(0)
          adClicked.adID=splited(1)
          adClicked.province=splited(2)
          adClicked.city=splited(3)
          adClicked.clickedCount=record._2
          adClickedList+=adClicked
        }
        
        val inserting=ListBuffer[AdClicked]()
        val updating=ListBuffer[AdClicked]()
        val jdbcWrapper=JDBCWrapper.getInstance()
        
        for(clicked<-adClickedList){
          def adClickedquerycallBack(result:ResultSet){
            while(result.next()){
              if(result.getRow!=0){
                val count=result.getLong(1)
                clicked.clickedCount=count//这里与上面的问题一样，究竟是谁错？
                updating+=clicked
              }else{
                inserting+=clicked
              }
            }
          }
          jdbcWrapper.doQuery("select count(1) from adclickedcount where timestamp=? and adID=? and province=? and city=?",
              Array(clicked.timestamp,clicked.adID,clicked.province,clicked.city), adClickedquerycallBack)
        }
        
        val insertParametersList = ListBuffer[paramsList]()
        for (insertRecord <- inserting) {
          val paramsListTmp = new paramsList()
          paramsListTmp.params1 = insertRecord.timestamp
          paramsListTmp.params2 = insertRecord.adID
          paramsListTmp.params3 = insertRecord.province
          paramsListTmp.params4 = insertRecord.city
          paramsListTmp.params10_Long = insertRecord.clickedCount
          paramsListTmp.params_Type = "adclickedcountInsert"
          insertParametersList += paramsListTmp
        }
        jdbcWrapper.doBatch("insert into adclickedcount values(?,?,?,?,?)", insertParametersList)

        val updateParametersList = ListBuffer[paramsList]()
        for (updateRecord <- updating) {
          val paramsListTmp = new paramsList()
          paramsListTmp.params1 = updateRecord.timestamp
          paramsListTmp.params2 = updateRecord.adID
          paramsListTmp.params3 = updateRecord.province
          paramsListTmp.params4 = updateRecord.city
          paramsListTmp.params10_Long = updateRecord.clickedCount
          paramsListTmp.params_Type = "adclickedcountUpdate"
          updateParametersList += paramsListTmp
        }
        jdbcWrapper.doBatch("update adclickedcount set clickedCount= ? where timestamp= ? and adID= ? and province= ? "
          + " and city= ?", updateParametersList)
      })
    })
    
    val updateStateByKeyDStreamrdd=updateStateByKeyDStream.transform(rdd=>{//此处是针对省份对广告进行处理
      val rowRDD=rdd.map(t=>{
        val splited=t._1.split("_")
        val timestamp="2016-09-03"
        val adID=splited(1)
        val province=splited(2)
        val clickedRecord=s"${timestamp}_${adID}_${province}"
        (clickedRecord,t._2)
      }).reduceByKey(_+_).map(v1=>{
        val splited=v1._1.split("_")
        val timestamp="2016-09-03"
        val adID=splited(1)
        val province=splited(2)
        Row(timestamp,adID,province,v1._2)
      })
      val structType=new StructType().add("timestamp",StringType)
      .add("adID",StringType).add("province",StringType).add("clickedCount",LongType)
      val hiveContext=new HiveContext(rdd.sparkContext)
      val df=hiveContext.createDataFrame(rowRDD, structType)
      df.registerTempTable("topNTableSource")
      val sqlText="select timestamp,adID,province,clickedCount from "+//这里注要是获得每个省份top5的广告点击排名
      " (select timestamp,adID,province,clickedCount,row_number() over (partition by province order by clickedCount desc) rank from topNTableSource) subquery" + 
      " where rank<=5"//这里使用了hive的开窗函数，具体百度，语句中rank应该指的是 row_number()函数列的别名,subquery是子查询后表的别名，并不实际是关键字
      val result=hiveContext.sql(sqlText)
      result.rdd
    })
    updateStateByKeyDStreamrdd.print()
    
    updateStateByKeyDStreamrdd.foreachRDD(rdd=>{
      rdd.foreachPartition(t=>{
        val adProvinceTopN=ListBuffer[AdProvinceTopN]()
        while(t.hasNext){
          val row=t.next()
          val item=new AdProvinceTopN()
          item.timestamp=row.getString(0)
          item.adID=row.getString(1)
          item.province=row.getString(2)
          item.clickedCount=row.getLong(3)
          adProvinceTopN+=item
        }
        val jdbcWrapper=JDBCWrapper.getInstance()
        val set=new scala.collection.mutable.HashSet[String]()
        for(itemTopn<-adProvinceTopN){
          set+=itemTopn.timestamp+"_"+itemTopn.province
        }
        val deleteParametersList=ListBuffer[paramsList]()
        for(deleteRecord<-set){
          val splited=deleteRecord.split("_")
          val paramsListTmp=new paramsList()
          paramsListTmp.params1=splited(0)
          paramsListTmp.params2=splited(1)
          paramsListTmp.params_Type="adprovincetopnDelete"
          deleteParametersList+=paramsListTmp
        }
        jdbcWrapper.doBatch("delete from adprovincetopn where timestamp=? and province=?", deleteParametersList)//清除对应省份的旧数据
        
        val insertParametersList=ListBuffer[paramsList]()
        for(updateRecord<-adProvinceTopN){
          val paramsListTmp=new paramsList()
          paramsListTmp.params1=updateRecord.timestamp
          paramsListTmp.params2=updateRecord.adID
          paramsListTmp.params3=updateRecord.province
          paramsListTmp.params10_Long=updateRecord.clickedCount
          paramsListTmp.params_Type="adprovincetopnInsert"
          insertParametersList+=paramsListTmp
        }
        jdbcWrapper.doBatch("insert into adprovincetopn values (?,?,?,?)", insertParametersList)//重新插入对应省份的新数据
      })
    })
//    
//    val filteredadClickedStreamingpair=filteredadClickedStreaming.map(t=>{
//      val splited=t.value().split("\t")
//      val adID=splited(3)
//      val time=splited(0)
//      (time+"_"+adID,1L)
//    })
//    
//    filteredadClickedStreamingpair.reduceByKeyAndWindow(_+_, _-_, Seconds(1800), Seconds(60))
//         .foreachRDD(rdd=>{
//           rdd.foreachPartition(partition=>{
//             val adTrend=ListBuffer[AdTrendStat]()
//             while(partition.hasNext){
//               val record=partition.next()
//               val splited=record._1.split("_")
//               val time=splited(0)
//               val adID=splited(1)
//               val clickedCount=record._2
//               
//               val adTrendStat=new AdTrendStat()
//               adTrendStat.adID=adID
//               adTrendStat.clickedCount=clickedCount
//               adTrendStat._date=time
//               adTrendStat._hour=time
//               adTrendStat._minute=time
//               adTrend+=adTrendStat
//             }
//             val inserting=ListBuffer[AdTrendStat]()
//             val updating=ListBuffer[AdTrendStat]()
//             val jdbcWrapper=JDBCWrapper.getInstance()
//             for(clicked<-adTrend){
//               val adTrendCountHistory=new AdTrendCountHistory()
//               
//               def adTrendquerycallBack(result:ResultSet){}
//             }
//           })
//         })
    ssc.start()
    ssc.awaitTermination()
  }
}