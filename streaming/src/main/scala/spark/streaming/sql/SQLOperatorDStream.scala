package spark.streaming.sql

import spark.streaming.{StreamingContext, Duration, DStream, Time}
import spark.RDD
import spark.storage.StorageLevel

private[streaming]
abstract class SQLOperatorDStream[T: ClassManifest] (
  ssc: StreamingContext
  ) extends DStream[T](ssc) {

  override def compute(validTime: Time): Option[RDD[T]] = None

  def compute(validTime: Time, child : SQLOperatorDStream[T], amILeftParent : Boolean): Option[RDD[T]]

  def printRDD(rdd: RDD[T]) = {
    val first11 = rdd.take(11)
    first11.take(10).foreach(item => println("debug:" + item))
    if (first11.size > 10) println("...")
    println()
  }

  protected[streaming] def getOrCompute(time: Time, child : SQLOperatorDStream[T], amILeftParent : Boolean): Option[RDD[T]] = {
    // If this DStream was not initialized (i.e., zeroTime not set), then do it
    // If RDD was already generated, then retrieve it from HashMap
    generatedRDDs.get(time) match {

      // If an RDD was already generated and is being reused, then
      // probably all RDDs in this DStream will be reused and hence should be cached
      case Some(oldRDD) => Some(oldRDD)

      // if RDD was not generated, and if the time is valid
      // (based on sliding time of this DStream), then generate the RDD
      case None => {
        if (isTimeValid(time)) {
          compute(time, child, amILeftParent) match {
            case Some(newRDD) =>
              if (storageLevel != StorageLevel.NONE) {
                newRDD.persist(storageLevel)
                logInfo("Persisting RDD " + newRDD.id + " for time " + time + " to " + storageLevel + " at time " + time)
              }
              if (checkpointDuration != null && (time - zeroTime).isMultipleOf(checkpointDuration)) {
                newRDD.checkpoint()
                logInfo("Marking RDD " + newRDD.id + " for time " + time + " for checkpointing at time " + time)
              }
              generatedRDDs.put(time, newRDD)
              Some(newRDD)
            case None =>
              None
          }
        } else {
          None
        }
      }
    }
  }
}

