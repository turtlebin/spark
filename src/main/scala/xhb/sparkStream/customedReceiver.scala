package xhb.sparkStream

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.net.Socket
import java.util.Date

class CustomReceiver(host: String, port: Int,  isTimeOut: Boolean, sec: Int)
         extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging{
  
  override def onStart(): Unit = {
  // Start the thread that receives data over a connection
  new Thread("Socket Receiver") {
    override def run() { receive() }
  }.start()
}

override def onStop(){
  // There is nothing much to do as the thread calling receive()
  // is designed to stop by itself if isStopped() returns false
}
private def receive() {
  val _pattern: String = "yyyy-MM-dd HH:mm:ss SSS"
  val _format: SimpleDateFormat = new SimpleDateFormat(_pattern)
  val _isTimeOut: Boolean = isTimeOut
  val _sec :Int = sec

  var socket: Socket = null
  var userInput: String = null
  try {
    // Connect to host:port
    socket = new Socket(host, port)
    println(_format.format(new Date()))
    println("建立了链接\n")
    if(_isTimeOut) socket.setSoTimeout(_sec * 1000)

    // Until stopped or connection broken continue reading
    val reader = new BufferedReader(
      new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
    userInput = reader.readLine()
    //while(!isStopped && userInput != null) {
    while(!isStopped && userInput != null) {
      println(userInput)
      store(userInput)
      userInput = reader.readLine()
    }
    reader.close()
    socket.close()

    // Restart in an attempt to connect again when server is active again
    restart("Trying to connect again")
  } catch {
    case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
    case t: Throwable =>
      // restart if there is any other error
      restart("Error receiving data", t)
  }
}
}