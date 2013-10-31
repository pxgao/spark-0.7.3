package spark.streaming.examples

import java.io._
import java.net.{InetAddress,ServerSocket,Socket,SocketException}
import java.util.Random

import scala.actors.Actor._
import scala.actors.Actor

object NetworkStreamGenerator {
  var x :Server = null
  var y :Server = null
  var z :Server = null

  var m1 = 0
  var m2 = 3000

  var p = 30

  def main(args: Array[String]) {
    m1 = args(0).toInt
    m2 = args(1).toInt

    p = args(2).toInt

    x = new Server(9999, m1, 100)
    y = new Server(9998, m1, 100)
    z = new Server(9997, m2, 100)

    x.start()
    y.start()
    z.start()
  }

  var started = false

  val timerActor = actor{
    receive{
      case "start" =>
      {
        started = true
        var count = 0
        while(true){
          if(count%(p*2) == 0){
            x.mean = m1
            println("x.mean = " + x.mean)
          }
          if(count%(p*2) == p){
            x.mean = m2
            println("x.mean = " + x.mean)
          }
          count +=1
          Thread.sleep(1000)
        }
      }
    }
  }
}

class Server(port : Int, _mean:Double = 0.0, _sd : Double = 100.0) extends Thread{
  val rand = new Random(port)
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
      if(!NetworkStreamGenerator.started)
        NetworkStreamGenerator.timerActor ! "start"
      println("accepted conn:" + socket.getPort)
      var count = 0
      try {
        val out = new DataOutputStream(socket.getOutputStream())

        while (true) {
         val x = ( getGaussian() + "," + mean.toInt + "\n").getBytes()
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