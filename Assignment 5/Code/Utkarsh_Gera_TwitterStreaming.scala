import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.twitter._

import scala.collection.mutable.{ListBuffer, Map, HashMap}
import scala.util.Random

object TwitterStreaming {
  def main(args: Array[String]): Unit = {
    // Create spark configuration
    val config = new SparkConf().setAppName("Twitter").setMaster("local[*]")

    System.setProperty("twitter4j.oauth.consumerKey", "DummyConsumerKey")
    System.setProperty("twitter4j.oauth.consumerSecret", "DummyConsumerSecret")
    System.setProperty("twitter4j.oauth.accessToken", "DummyConsumerAccessToken")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "DummyAccessTokenSecret")

    // Stream context
    val ssc = new StreamingContext(config, Seconds(10))
    ssc.sparkContext.setLogLevel(logLevel = "OFF")

    // Sample storage
    val sample: ListBuffer[String] = ListBuffer()

    // Tweets Seen
    var s = 0

    // Random Counter
    val r = Random

    // Twitter Stream
    val stream = TwitterUtils.createStream(ssc, None)

    // Format the tweets
    val tweets_rdd = stream.map(row => row.getText)

    tweets_rdd.foreachRDD(rdd => {
      val tweets = rdd.collect()
      var index = 0

      // If sample is not filled up
      if(sample.length<=100) {
        // Pending sample size
        index = 100 - sample.length

        // Add the remaining size
        sample.appendAll(tweets.slice(0, index))

        // Update the tweets seen till now
        s = sample.length
      }

      // Now check the luck of all the other tweets
      while(index < tweets.length) {
        // Update the tweet seen
        s += 1

        // See the probability
        if(r.nextInt(s)<100) {
          val i = r.nextInt(100)
          sample(i) = tweets(index)
          toptags(sample, s)
        }

        // Update the index
        index += 1
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Function that prints the top 5 tags along with the average tweet length
    * @param tweets
    * @param s
    */
  def toptags(tweets: ListBuffer[String], s: Int): Unit = {
    println("The number of the twitter from beginning: " + s)
    println("Top 5 hot hashtags:")

    val hash_tag: Map[String, Int] = HashMap().withDefaultValue(0)
    var tweet_l = 0.0
    for(tweet <- tweets) {
      tweet_l += tweet.length

      // Format the tweets by removing new lines first and then divide them on the basis of the space
      for(potential_tag <- tweet.replace("\n", " ").split(" ").filter(_.startsWith("#"))) {
        // Check for multiple concatenated hash tags
        for(tag <- potential_tag.replaceFirst("#", "").split("#")) {
          hash_tag(tag) += 1
        }
      }
    }

    val hashtags = hash_tag.toList.sortBy(_._2).reverse.take(5)
    for(tag <- hashtags) {
      println(tag._1 + ":" + tag._2)
    }
    println("The average length of the twitter is: " + tweet_l/tweets.length)
  }
}
