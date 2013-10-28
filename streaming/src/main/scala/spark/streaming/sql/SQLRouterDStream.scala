package spark.streaming.sql

import spark.RDD
import spark.streaming._
import scala.collection.mutable.ArrayBuffer
import spark.streaming.dstream.ForEachDStream
import scala.Some
import spark.streaming.Duration
import spark.storage.StorageLevel


class SQLRouterDStream[T : ClassManifest](
  ssc : StreamingContext
  ) extends SQLOperatorDStream[T](ssc) {

  val attachedSQLDStreams = ArrayBuffer[SQLOperatorDStream[T]]()
  val foreachSQLDStreams = ArrayBuffer[SQLForEachDStream[T]]()
  val parents = scala.collection.mutable.HashMap[SQLOperatorDStream[T],(SQLOperatorDStream[T],Option[SQLOperatorDStream[T]])]()
  var isRouterInitialized = false

  override def dependencies = List()

  override def slideDuration: Duration = {
    if (ssc == null) throw new Exception("ssc is null")
    if (ssc.graph.batchDuration == null) throw new Exception("batchDuration is null")
    ssc.graph.batchDuration
  }

  protected[streaming] override def initialize(time: Time) {
    super.initialize(time)
    if(!isRouterInitialized){
      isRouterInitialized = true
      attachedSQLDStreams.foreach(stream => {
          if(!stream.isInstanceOf[SQLForEachDStream[T]])
            stream.initialize(time)
        }
      )

    }

  }

  override def compute(validTime: Time, child : SQLOperatorDStream[T], amILeftParent : Boolean): Option[RDD[T]] = {
    if(child.isInstanceOf[SQLUnaryTransformedDStream[T]] || child.isInstanceOf[SQLForEachDStream[T]])
      parents(child)._1.getOrCompute(validTime,child,false)
    else if(child.isInstanceOf[SQLBinaryTransformedDStream[T]]){
      amILeftParent match {
        case true => parents(child)._1.getOrCompute(validTime,child,false)
        case false => parents(child)._2.get.getOrCompute(validTime,child,false)
      }
    }
    else
      throw new Exception("Wrong child type")
  }


  protected[streaming] override def getOrCompute(time: Time, child : SQLOperatorDStream[T], amILeftParent : Boolean): Option[RDD[T]] = {
    compute(time, child, amILeftParent)
  }


  def addParsedDStream(parent: DStream[_], transformFunc: RDD[_] => RDD[T]) : SQLOperatorDStream[T] = {
    val stream = new SQLParsedDStream[T](parent, context.sparkContext.clean(transformFunc))
    attachedSQLDStreams += stream
    stream
  }

  def addUnaryTransformedDStream(parent: SQLOperatorDStream[T], transformFunc: RDD[T] => RDD[T]) : SQLOperatorDStream[T] = {
    val stream = new SQLUnaryTransformedDStream[T](this, context.sparkContext.clean(transformFunc))
    attachedSQLDStreams += stream
    parents += (stream -> (parent, None))
    stream
  }

  def addBinaryTransformedDStream(leftParent: SQLOperatorDStream[T],
                                  rightParent: SQLOperatorDStream[T],
                                  func : ((RDD[T], RDD[T]) => RDD[T])) : SQLOperatorDStream[T] = {
    val stream = new SQLBinaryTransformedDStream[T](this, this, context.sparkContext.clean(func))
    attachedSQLDStreams += stream
    parents += (stream -> (leftParent, Some(rightParent)))
    stream
  }

  def addForEachDStream(parent: SQLOperatorDStream[T],
                        foreachFunc: (RDD[T], Time) => Unit): SQLOperatorDStream[T] = {
    val stream = new SQLForEachDStream[T](this, context.sparkContext.clean(foreachFunc))
    attachedSQLDStreams += stream
    parents += (stream -> (parent, None))
    stream
  }

  def addPrintDStream(parent : SQLOperatorDStream[T]): SQLOperatorDStream[T] = {
    def foreachFunc = (rdd: RDD[T], time: Time) => {
      val first11 = rdd.take(11)
      println ("-------------------------------------------")
      println ("Time: " + time)
      println ("-------------------------------------------")
      first11.take(10).foreach(println)
      if (first11.size > 10) println("...")
      println()
    }
    addForEachDStream(parent, foreachFunc)
  }

}
