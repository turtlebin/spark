package xhb.SparkCommerce

import java.util.concurrent.LinkedBlockingQueue
import java.sql.Connection
import java.sql.DriverManager
import scala.collection.mutable.ListBuffer
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.ResultSet

object JDBCWrapper{
  private var jdbcInstance:JDBCWrapper=_
  def getInstance():JDBCWrapper={
    synchronized{
      if(jdbcInstance==null){
        jdbcInstance=new JDBCWrapper()
      }
    }
    jdbcInstance
  }
}

class JDBCWrapper {
  val dbConnectionPool=new LinkedBlockingQueue[Connection]()
  try{
    Class.forName("com.mysql.jdbc.Driver")
  }catch{
    case e:Exception=>e.printStackTrace()
  }
  
  for(i<-1 to 10 ){
    try{
      val conn=DriverManager.getConnection("jdbc:mysql://Master:3306/sparkstreaming","root","csasc")
      dbConnectionPool.put(conn)
    }catch{
      case e:Exception=>e.printStackTrace()
    }
  }
  
  def getConnection()=synchronized{
    while(0==dbConnectionPool.size()){
      try{
        Thread.sleep(20)
      }catch{
        case e:InterruptedException=>e.printStackTrace()
      }
    }
    dbConnectionPool.poll()
  }

  def doBatch(sqlText: String, paramsList: ListBuffer[paramsList]) = {
    val conn = getConnection
    var preparedStatement: PreparedStatement = null
    val result: Array[Int] = null
    try {
      conn.setAutoCommit(false)
      preparedStatement = conn.prepareStatement(sqlText)
      for (parameters <- paramsList) {
        parameters.params_Type match {
          case "adclickedInsert" => {
//            println("adclickedInsert")
            preparedStatement.setObject(1, parameters.params1)
            preparedStatement.setObject(2, parameters.params2)
            preparedStatement.setObject(3, parameters.params3)
            preparedStatement.setObject(4, parameters.params4)
            preparedStatement.setObject(5, parameters.params5)
            preparedStatement.setObject(6, parameters.params6)
            preparedStatement.setObject(7, parameters.params10_Long)
          }

          case "blacklisttableInsert" => {
//            println("blacklisttableInsert")
            preparedStatement.setObject(1, parameters.params1)
          }

          case "adclickedcountInsert" => {
//            println("adclickedcountInsert")
            preparedStatement.setObject(1, parameters.params1)
            preparedStatement.setObject(2, parameters.params2)
            preparedStatement.setObject(3, parameters.params3)
            preparedStatement.setObject(4, parameters.params4)
            preparedStatement.setObject(5, parameters.params10_Long)
          }

          case "adclickedtrendInsert" => {
//            println("adclickedtrendInsert")
            preparedStatement.setObject(1, parameters.params1)
            preparedStatement.setObject(2, parameters.params2)
            preparedStatement.setObject(3, parameters.params3)
            preparedStatement.setObject(4, parameters.params4)
            preparedStatement.setObject(5, parameters.params10_Long)
          }

          case "adprovincetopnInsert" => {
//            println("adprovincetopnInsert")
            preparedStatement.setObject(1, parameters.params1)
            preparedStatement.setObject(2, parameters.params2)
            preparedStatement.setObject(3, parameters.params3)
            preparedStatement.setObject(4, parameters.params10_Long)
          }

          case "adclickedUpdate" => {
//            println("adclickedUpdate")
            preparedStatement.setObject(1, parameters.params10_Long)
            preparedStatement.setObject(2, parameters.params1)
            preparedStatement.setObject(3, parameters.params2)
            preparedStatement.setObject(4, parameters.params3)
            preparedStatement.setObject(5, parameters.params4)
            preparedStatement.setObject(6, parameters.params5)
            preparedStatement.setObject(7, parameters.params6)
          }

          case "blacklisttableUpdate" => {
//            println("blacklisttableUpdate")
            preparedStatement.setObject(1, parameters.params1)
          }

          case "adclickedcountUpdate" => {
//            println("adclickedcountUpdate")
            preparedStatement.setObject(1, parameters.params10_Long)
            preparedStatement.setObject(2, parameters.params1)
            preparedStatement.setObject(3, parameters.params2)
            preparedStatement.setObject(4, parameters.params3)
            preparedStatement.setObject(5, parameters.params4)
          }

          case "adprovincetopnUpdate" => {
//            println("adprovincetopnUpdate")
            preparedStatement.setObject(1, parameters.params10_Long)
            preparedStatement.setObject(2, parameters.params1)
            preparedStatement.setObject(3, parameters.params2)
            preparedStatement.setObject(4, parameters.params3)
          }

          case "adprovincetopnDelete" => {
//            println("adprovincetopnDelete")
            preparedStatement.setObject(1, parameters.params1)
            preparedStatement.setObject(2, parameters.params2)
          }

          case "adclickedtrendUpdate" => {
//            println("adclickedtrendUpdate")
            preparedStatement.setObject(1, parameters.params10_Long)
            preparedStatement.setObject(2, parameters.params1)
            preparedStatement.setObject(3, parameters.params2)
            preparedStatement.setObject(4, parameters.params3)
            preparedStatement.setObject(5, parameters.params4)
          }
        }
        preparedStatement.addBatch()
      }

      val result = preparedStatement.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (preparedStatement != null) {
        try {
          preparedStatement.close()
        } catch {
          case e: SQLException => e.printStackTrace()
        }
      }
      if (conn != null) {
        try {
          dbConnectionPool.put(conn)
        } catch {
          case e: InterruptedException => e.printStackTrace()
        }
      }
    }
    result
  }
  
  def doQuery(sqlText:String,paramsList:Array[_],callBack:ResultSet=>Unit){
    val conn:Connection=getConnection()
    var preparedStatement:PreparedStatement=null
    var result:ResultSet=null
    try{
      preparedStatement=conn.prepareStatement(sqlText)
      if(paramsList!=null){
        for(i <- 0 to paramsList.length-1){
          preparedStatement.setObject(i+1, paramsList(i))
        }
      }
      result=preparedStatement.executeQuery()
      callBack(result)
    }catch{
      case e:Exception=>e.printStackTrace()
    }finally{
      if(preparedStatement!=null){
        try{
          preparedStatement.close()
        }catch{
          case e:SQLException=>e.printStackTrace()
        }
      }
      if(conn!=null){
        try{
          dbConnectionPool.put(conn)
        }catch{
          case e:InterruptedException=>e.printStackTrace()
        }
      }
    }
  }
  
  def resultCallBack(result:ResultSet,blackListNames:List[String]):Unit={
    
  }
  
}

class paramsList extends Serializable{
  var params1:String=_
  var params2:String=_
  var params3:String=_
  var params4:String=_
  var params5:String=_
  var params6:String=_
  var params7:String=_
  var params10_Long:Long=_
  var params_Type:String=_
  var length:Int=_      
}

class UserAdClicked extends Serializable{
  var timestamp:String=_
  var ip:String=_
  var userID:String=_
  var adID:String=_
  var province:String=_
  var city:String=_
  var clickedCount:Long=_
  
  override def toString:String=s"UserAdClicked[timestamp=${timestamp},ip=${ip},userID=${userID},adID=${adID},province=${province},city=${city},clickedCount=${clickedCount}]"
  
}

class AdClicked extends Serializable{
  var timestamp:String=_
  var adID:String=_
  var province:String=_
  var city:String=_
  var clickedCount:Long=_
  override def toString:String=s"AdCLicked[timestamp=${timestamp},adID=${adID},province=${province},city=${city},clickedCount=${clickedCount}]"
}

class AdProvinceTopN extends Serializable{
  var timestamp:String=_
  var adID:String=_
  var province:String=_
  var clickedCount:Long=_
}

class AdTrendStat extends Serializable{
  var _date:String=_
  var _hour:String=_
  var _minute:String=_
  var adID:String=_
  var clickedCount:Long=_
  override def toString:String=s"AdTrendStat[_date=${_date},_hour=${_hour},_mintute=${_minute},adID=${adID},clickedCount=${clickedCount}]"
}

class AdTrendCountHistory extends Serializable{
  var clickedCountHistor:Long=_
}