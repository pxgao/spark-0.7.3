import spark.SparkContext
import SparkContext._

object SimpleJob{
	def main(args: Array[String]) {
		val logFile = "/var/log/syslog" 
		val sc = new SparkContext("local", "Simple Job", "$YOUR_SPARK_HOME", List("target/scala-2.9.2/simple-project_2.9.2-1.0.jar"))
		val logData = sc.textFile(logFile, 2).cache()
		val numAs = logData.filter(_.contains("a")).count()
		val numBs = logData.filter(_.contains("b")).count()
		println("A:%s, B:%s".format(numAs, numBs))
	}
}


