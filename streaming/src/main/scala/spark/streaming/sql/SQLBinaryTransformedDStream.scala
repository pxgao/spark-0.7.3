package spark.streaming.sql

import spark.RDD
import spark.streaming.{Time, Duration}

private[streaming]
class SQLBinaryTransformedDStream[T : ClassManifest](
  leftParent: SQLOperatorDStream[T],
  rightParent: SQLOperatorDStream[T],
  func : ((RDD[T], RDD[T]) => RDD[T])
  ) extends SQLOperatorDStream[T](leftParent.ssc) {

  if (leftParent.ssc != rightParent.ssc) {
    throw new IllegalArgumentException("Parents have different StreamingContexts")
  }

  if (leftParent.slideDuration != rightParent.slideDuration) {
    throw new IllegalArgumentException("Parents have different slide times")
  }

  override def dependencies = List(leftParent,rightParent)

  override def slideDuration: Duration = leftParent.slideDuration

  override def compute(validTime: Time, child : SQLOperatorDStream[T], amILeftParent : Boolean): Option[RDD[T]] = {


    val leftRDD = leftParent.getOrCompute(validTime, this, true)
    val rightRDD = rightParent.getOrCompute(validTime, this, false)


    if(leftRDD == None || rightRDD == None)
      None
    else
      Some(func(leftRDD.get, rightRDD.get))
  }
}
