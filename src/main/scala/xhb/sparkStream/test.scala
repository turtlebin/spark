import java.io.OutputStreamWriter
import java.net.SocketTimeoutException
import java.text.SimpleDateFormat
import java.net.ServerSocket
import java.net.SocketException
import java.util.Date
import java.util.Scanner

class CustomServer(port: Int, isTimeOut: Boolean ,sec: Int) {
  val _pattern: String = "yyyy-MM-dd HH:mm:ss SSS"
  val format: SimpleDateFormat = new SimpleDateFormat(_pattern)
  val _isTimeOut = isTimeOut
  val _sec: Int = sec
  val _port = port

  def onStart(): Unit = {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() {
        sServer()
      }
    }.start()
  }

  def onStop(): Unit = {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  def sServer(): Unit = {
    println("----------Server----------")
    println(format.format(new Date()))
    var tryingCreateServer = 1
    try {
      val server = new ServerSocket(_port)
      println("监听建立 等你上线\n")
      if(_isTimeOut) server.setSoTimeout(_sec)

      val socket = server.accept()
      println(format.format(new Date()))
      println("与客户端建立了链接")

      val writer = new OutputStreamWriter(socket.getOutputStream)

      println(format.format(new Date()))
      val in = new Scanner(System.in)
      //这里只是设置\n为数据分隔符，默认是空格
      in.useDelimiter("\n")
      println("请写入数据")
      var flag = in.hasNext
      while (flag){
        val s = in.next()

        /**
          * 注意：writer写入s数据，如果不加\n那么客户端接收不到数据
          */
        writer.write(s + "\n")
        Thread.sleep(1000)
        if(socket.isClosed){
          println("socket is closed !")
        }else{
          try{
            writer.flush()
          }catch {
            case e: java.net.SocketException =>
              println("Error 客户端连接断开了！！！！！！！！！")
              flag = false
              writer.close()
              socket.close()
              server.close()
              onStart()
              return
          }
        }
      }
      System.out.println(format.format(new Date()))
      System.out.println("写完啦 你收下\n\n\n\n\n")

      /**
        * 重新尝试建立监听
        */
      if(tryingCreateServer < 5){
        writer.close()
        socket.close()
        server.close()
        onStart()
        tryingCreateServer += 1
      }

    } catch{
      case e: SocketTimeoutException =>
        System.out.println(format.format(new Date()) + "\n" + _sec + "秒没给我数据 我下啦\n\n\n\n\n");
        e.printStackTrace()
      case e: SocketException =>
        e.printStackTrace()
      case e: Exception =>
        e.printStackTrace()
    }
  }
}

object CustomServer {
  def main(args: Array[String]): Unit = {
    new CustomServer(8888, false, 0).onStart()
  }
}
