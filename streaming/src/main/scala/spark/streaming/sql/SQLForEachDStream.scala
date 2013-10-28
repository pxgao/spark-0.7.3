package spark.streaming.sql

import spark.RDD
import spark.streaming.{Duration, DStream, Job, Time}

private[streaming]
class SQLForEachDStream[T: ClassManifest] (
    parent: SQLOperatorDStream[T],
    foreachFunc: (RDD[T], Time) => Unit
  ) extends SQLOperatorDStream[T](parent.ssc) {
  ssc.registerOutputStream(this)

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time, child : SQLOperatorDStream[T], amILeftParent : Boolean): Option[RDD[T]] = None

  override def generateJob(time: Time): Option[Job] = {
    parent.getOrCompute(time, this, true) match {
      case Some(rdd) =>
        val jobFunc = () => {
          foreachFunc(rdd, time)
        }
        Some(new Job(time, jobFunc))
      case None => None
    }
  }

}
