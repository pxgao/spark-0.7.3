package spark.streaming.examples

import spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import spark.SparkContext._
import twitter4j._
import twitter4j.auth.Authorization
import java.util.prefs.Preferences
import twitter4j.conf.ConfigurationBuilder
import twitter4j.conf.PropertyConfiguration
import twitter4j.auth.OAuthAuthorization
import twitter4j.auth.AccessToken

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 */
object TwitterPopularTags {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: TwitterPopularTags <master>" +
        " [filter1] [filter2] ... [filter n]")
      System.exit(1)
    }

    val (master, filters) = (args.head, args.tail)

    val ssc = new StreamingContext(master, "TwitterPopularTags", Seconds(2),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    val conf = new ConfigurationBuilder()
    conf.setOAuthConsumerKey("ELMGMO39Ao2xtR6eVFWB7A")
    conf.setOAuthConsumerSecret("3HE6G6Nf92nknVvmcaOFugKPT90Er6LnNwP4RvI")
    conf.setOAuthAccessToken("46639229-c9OGp8vgFRSkpWKfTodTlq1bR8mH8drTNv6Ma55Ni")
    conf.setOAuthAccessTokenSecret("I0b70CwaQizXUtupo7hFCgidTaarqJ8FkvSBAymyhlaUG")

    val auth = new OAuthAuthorization(conf.build)
    val stream = ssc.twitterStream(Some(auth), filters)

    val t = stream.map(s => (s.getUser.getScreenName,s.getText,s.getInReplyToScreenName,s.getInReplyToStatusId, s.getInReplyToUserId))

    t.print()
//    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

//    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
//                     .map{case (topic, count) => (count, topic)}
//                     .transform(_.sortByKey(false))
//
//    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
//                     .map{case (topic, count) => (count, topic)}
//                     .transform(_.sortByKey(false))
//
//
//    // Print popular hashtags
//    topCounts60.foreach(rdd => {
//      val topList = rdd.take(5)
//      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
//      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
//    })
//
//    topCounts10.foreach(rdd => {
//      val topList = rdd.take(5)
//      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
//      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
//    })

    ssc.start()
  }
}
