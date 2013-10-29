package spark.streaming.examples

import java.io._
import java.net.{InetAddress,ServerSocket,Socket,SocketException}
import java.util.Random


object NetworkStreamGenerator {
  def main(args: Array[String]) {
    val x = new Server(9999, args(0).toInt, 1000)
    val y = new Server(9998, args(1).toInt, 1000)
    val z = new Server(9997, args(2).toInt, 1000)
    x.start()
    y.start()
    z.start()
  }
}

class Server(port : Int, _mean:Double = 0.0, _sd : Double = 100.0) extends Thread{
  val rand = new Random(System.currentTimeMillis())
  var mean = _mean;
  var sd = _sd;

  def getGaussian() : Int = {
    (rand.nextGaussian() * sd + mean).toInt
  }

  override def run() :Unit = {
    try {
      println("listening "  + port)
      val listener = new ServerSocket(port)
      while (true){
        new ServerThread(listener.accept()).start()
        println("rq from " + port)
      }
      listener.close()
    }
    catch {
      case e: IOException =>
        System.err.println("Could not listen on port: " + port)
        System.exit(-1)
    }
  }

  case class ServerThread(socket: Socket) extends Thread("ServerThread") {

    override def run(): Unit = {
      println("accepted conn:" + socket.getPort)
      var count = 0
      try {
        val out = new DataOutputStream(socket.getOutputStream())

        while (true) {
         val x = ( getGaussian() + "," + 1 + "\n").getBytes()
         out.write(x)
          count += 1
         //print(new String(x))
         if(count%5 == 0)
          Thread.sleep(1)
        }

        out.close()
        socket.close()
      }
      catch {
        case e: SocketException =>
          () // avoid stack trace when stopping a client with Ctrl-C
        case e: IOException =>
          e.printStackTrace()
      }
    }
  }
}