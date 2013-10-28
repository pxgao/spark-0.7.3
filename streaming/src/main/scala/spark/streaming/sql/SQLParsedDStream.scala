package spark.streaming.sql

import spark.RDD
import spark.streaming.{DStream, Duration, Time}

private[streaming]
class SQLParsedDStream[T: ClassManifest] (
    parent: DStream[_],
    transformFunc: RDD[_] => RDD[T]
  ) extends SQLOperatorDStream[T](parent.ssc) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time, child : SQLOperatorDStream[T], amILeftParent : Boolean): Option[RDD[T]] = {
    parent.getOrCompute(validTime).map(transformFunc(_))
  }
}
