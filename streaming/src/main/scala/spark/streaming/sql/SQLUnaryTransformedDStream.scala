package spark.streaming.sql

import spark.RDD
import spark.streaming.{Duration, DStream, Time}

private[streaming]
class SQLUnaryTransformedDStream[T: ClassManifest] (
    parent: SQLOperatorDStream[T],
    transformFunc: RDD[T] => RDD[T]
  ) extends SQLOperatorDStream[T](parent.ssc) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time, child : SQLOperatorDStream[T], amILeftParent : Boolean): Option[RDD[T]] = {
    parent.getOrCompute(validTime, this, true).map(transformFunc(_))
  }
}
