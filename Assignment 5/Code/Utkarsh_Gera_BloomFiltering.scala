import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.Set

object BloomFiltering {
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

    // Twitter Stream
    val stream = TwitterUtils.createStream(ssc, None)

    // Bloom filter of 64 bits and the hash set
    val bloom_filter: Array[Int] = Array.fill(64)(0)
    val hash_tags: Set[Int] = Set()
    var false_positives: Int = 0

    // Format the tweets by removing new lines first and then divide them on the basis of the space
    val tweet_hash_tags_rdd = stream.flatMap(row => row.getText.replace("\n", " ").split(" ").filter(row => row.startsWith("#")))

    // Iterate over all the
    tweet_hash_tags_rdd.foreachRDD(rdd => {
      val tags = rdd.collect()

      // Initialize the variables for this chunk
      var correct: Int = 0
      var incorrect: Int = 0

      // Iterate over all the tags
      for(tag <- tags) {
        // Check for multiple concatenated hash tags
        val all_tags = tag.replaceFirst("#", "").split("#")

        for(t <- all_tags){
          // Calculate the hash value
          val tweetint: Int = t.hashCode

          // Need to take absolute since array indices can't be negative
          val a = math.abs(hash1(tweetint))
          val b = math.abs(hash2(tweetint))

          // Apply bloom filter
          if((bloom_filter(a) & bloom_filter(b)) < 1) {
            // Update the filters
            bloom_filter(a) = 1
            bloom_filter(b) = 1

            correct += 1
          } else {
            if(!hash_tags.contains(tweetint)) {
              incorrect += 1
            } else {
              correct += 1
            }
          }

          hash_tags.add(tweetint)
        }
      }

      // Update the false positives
      false_positives += incorrect

      println("> Local statistics for this chunk -")
      println("Number of Correct estimation in this chunk: " + correct)
      println("Number of Incorrect estimation/False Positives in this chunk: " + incorrect)
      println("> Global statistics from starting till this chunk -")
      println("Number of actual unique hash tags encountered up till now: " + hash_tags.size)
      println("Number of False Positives by the bloom filter up till now: " + false_positives)
      println("------------------------ End of this chunk ------------------------")
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Hash function 1 - Modulo 64
    * @param num
    * @return the modulo value
    */
  def hash1(num: Int): Int = {
    return num % 64
  }

  /**
    * Hash Function 2 - Modulo 59
    * @param num
    * @return the modulo value
    */
  def hash2(num: Int): Int = {
    return num % 59
  }
}
