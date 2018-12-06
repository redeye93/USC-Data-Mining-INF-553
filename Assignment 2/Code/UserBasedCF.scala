import java.nio.file.{Files, Paths}

import ModelBasedCF.{getData, writeFile}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}
import scala.collection.Set
import scala.math.sqrt
import scala.reflect.classTag

object UserBasedCF {
  def main(args: Array[String]): Unit = {
    /**
      * Pre-setting
      */
    if( args.size!=2 ) {
      print("Need at two arguments - Input rating file and test file")
      return
    } else {
      if ( !Files.exists(Paths.get(args(0))) ) {
        print("Input file doesn't exist")
        return
      }
      else if ( !Files.exists(Paths.get(args(1))) ) {
        print("Testing file doesn't exist")
        return
      }
    }

    // Record time
    val start_time = System.nanoTime()

    // Create spark configuration
    val config = new SparkConf().setAppName("ModelBasedCF").setMaster("local")
    val sc: SparkContext = new SparkContext(config)
    // Edit TODO
    //sc.setCheckpointDir(Paths.get(args(0)).getParent.toString)

    val rdd_train: RDD[Array[String]] = getData(sc, args(0))

    // Create maps of user ids and business ids in training
    val (user_avg_mod_hash: Map[String, (Double, Double, Boolean)], item_avg_hash: Map[String, (Set[String], Double)],
    user_item_hash: Map[String, Map[String, Double]]) = generateMap(rdd_train)

    /**
      * Test part
      */
    val rdd_test = getData(sc, args(1))

    // Pre-processing part. Store the rate that is once computed because it does't change
    val weights: Map[String, Map[String, Double]] = HashMap()
    val co_rated_items: Map[String, Map[String, Set[String]]] = HashMap()

    // Map Reduce for every user and business Id might give fast results
    val result_rdd: RDD[((String, String), Double)] = rdd_test.map{row =>
      // If the data for both the elements is present
      if(!user_avg_mod_hash.contains(row(0)) && !item_avg_hash.contains(row(1))) {
        // Place the average
        (((row(0), row(1)), 2.5))
      } else if(!user_avg_mod_hash.contains(row(0))) {
        // Place the average rating of business by different users
        (((row(0), row(1)), item_avg_hash(row(1))._2))
      } else if(!item_avg_hash.contains(row(1)) || user_avg_mod_hash(row(0))._3) {
        // Place the average of this user's rating
        (((row(0), row(1)), user_avg_mod_hash(row(0))._1))
      } else {
        // Check if the rating already exists
        if(user_item_hash(row(0)).contains(row(1))) {
          (((row(0), row(1)), user_item_hash(row(0))(row(1))))
        } else {
          // Find similar users for this user by calculating the Cosine similarity
          if(!weights.contains(row(0))) {
            // Similarity Hash Creation
            val similarity_hash: Map[String, Double] = calculateSimilarity(row(0), user_item_hash, item_avg_hash, user_avg_mod_hash)
            val temp = calculateWeights(row(0), user_avg_mod_hash, similarity_hash, user_item_hash)
            weights(row(0)) = temp._1
            co_rated_items(row(0)) = temp._2
          }
          // We have already computed the weights before, so now lets use them to predict the rating
          ((row(0), row(1)), predictRating(row(0), row(1), weights(row(0)), user_item_hash, item_avg_hash,
            co_rated_items(row(0)), user_avg_mod_hash(row(0))._1))
        }
      }
    }

    /**
      * Write the output file
      **/
    val formatted_output: String = formatOutput(result_rdd)
    writeFile(formatted_output, "UserBasedCF", ".")
    print(Paths.get(".").toString)

    // Join the predictions and the actual values
    val ratesAndPreds = rdd_test.map((x: Array[String]) =>
      ((x(0), x(1)), x(2).toDouble)).join(result_rdd)

    //Calculate the error difference
    val diff = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err: Double = (r1 - r2)
      if(err<1) {
        (0, 1)
      } else if(err<2) {
        (1, 1)
      } else if(err<3) {
        (2, 1)
      } else if(err<4) {
        (3, 1)
      } else {
        (4, 1)
      }
    }.reduceByKey((a, b) => a+b).sortBy(_._1).collect()

    val diff_map: Map[Int, Int] = HashMap()
    for(ele <- diff) {
      diff_map(ele._1) = ele._2
    }

    for(i: Int <- 0 to 3) {
      var d = 0
      if(diff_map.contains(i)){
        d = diff_map(i)
      }
      println(">=" + i + " and <" + (i+1) + ": " + d)
    }
    var d = 0
    if(diff_map.contains(4)) d = diff_map(4)
    println(">=4: " + d)

    // Calculate the error
    val RMSE = sqrt(ratesAndPreds.map { case ((_, _), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean())
    println(s"RMSE: $RMSE")
    println("Time: " + ((System.nanoTime()-start_time)/1000000000 ) + " sec")
  }

  def generateMap(rdd_survey: RDD[Array[String]]): (Map[String, (Double, Double, Boolean)], Map[String, (Set[String], Double)], Map[String, Map[String, Double]]) = {
    // Key, (sum, square, count)
    val user_average_mod: Array[(String, (Double, Double, Int))] = rdd_survey.map(row => (row(0),
      (row(2).toDouble, math.pow(row(2).toFloat, 2), 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)).collect()

    // Generate a hash map for users average and mod of its vector and store if we need to calculate its weight with other similar users
    val user_avg_mod_hash: Map[String, (Double, Double, Boolean)] = HashMap()
    for(user: (String, (Double, Double, Int)) <- user_average_mod) {
      user_avg_mod_hash(user._1) = (user._2._1 / user._2._3, math.sqrt(user._2._2), user._2._3 > 20)
    }

    // Calculate the average of the item ratings
    val item_average: Array[(String, (Set[String], Double, Int))] = rdd_survey.map(row => (row(1), (Set(row(0)), row(2).toDouble, 1))).reduceByKey((a, b) =>
      (a._1 ++ b._1, a._2 + b._2, a._3 + b._3)).collect()

    // Generate a hash map for items user set and average
    val item_avg_hash: Map[String, (Set[String], Double)] = HashMap()
    for(item: (String, (Set[String], Double, Int)) <- item_average) {
      item_avg_hash(item._1) = (item._2._1, (item._2._2 / item._2._3))
    }

    // Create a user item hash map using map reduce
    val user_item_map: RDD[(String, (Map[String, Double]))] = rdd_survey.map(row => (row(0), Map((row(1),
      row(2).toDouble)))).reduceByKey((a, b) => a ++ b)

    // Convert into hashmap
    val user_item_hash: Map[String, Map[String, Double]] = HashMap()
    for(row: (String, Map[String, Double]) <- user_item_map.collect()) {
      user_item_hash(row._1) = row._2
    }

    return (user_avg_mod_hash, item_avg_hash, user_item_hash)
  }

  def calculateSimilarity(active_user: String, user_item_hash: Map[String, Map[String, Double]],
                          item_avg_hash: Map[String, (Set[String], Double)],
                          user_avg_mod_hash: Map[String, (Double, Double, Boolean)]) : Map[String, Double] = {
    val rated_items: Set[String] = user_item_hash(active_user).keySet

    // Create a set of users who have rated all the items as rated by active user
    var users: Set[String] = Set()
    for(item: String <- rated_items) {
      users = users ++ item_avg_hash(item)._1
    }

    // Filter out the users have rated more than 5 items and have at least co-rated 1 items with active user
    val final_user_list: ArrayBuffer[String] = ArrayBuffer()
    for(pot_users <- users) {
      if(user_avg_mod_hash(pot_users)._3 && rated_items.intersect(user_item_hash(pot_users).keySet).size > 12) {
        final_user_list += pot_users
      }
    }

    // Create a hash for each user of similarity
    val similarity_hash: Map[String, Double] = HashMap()
    for(user <- final_user_list) {
      var item_list: Set[String] = Set()
      if(rated_items.size < user_item_hash(user).keySet.size) {
        item_list = rated_items
      } else {
        item_list = user_item_hash(user).keySet
      }

      var numerator: Double = 0.0
      for(item <- item_list) {
        numerator += (if (user_item_hash(active_user).contains(item)) user_item_hash(active_user)(item) else 0.0) *
          (if (user_item_hash(user).contains(item)) user_item_hash(user)(item) else 0.0)
      }

      similarity_hash(user) = numerator / (user_avg_mod_hash(user)._2 * user_avg_mod_hash(active_user)._2)
    }

    return similarity_hash
  }

  def calculateWeights(active_user: String, user_avg_mod_hash: Map[String, (Double, Double, Boolean)],
                       similarity_hash: Map[String, Double], user_item_hash: Map[String, Map[String, Double]])
  : (Map[String, Double], Map[String, Set[String]]) = {
    val result: Map[String, Double] = HashMap()
    val rated_items: Set[String] = user_item_hash(active_user).keySet
    val co_rated_items: Map[String, Set[String]] = HashMap()

    // Calculate the rating difference for a user for all of his rated items and collect them
    for(user: String <- similarity_hash.keySet) {
      if(similarity_hash(user) > 0.5) {
        val co_items = rated_items.intersect(user_item_hash(user).keySet)
        co_rated_items(user) = co_items
        var active_u_avg, other_u_avg = 0.0

        for(item <- co_items) {
          active_u_avg += user_item_hash(active_user)(item)
          other_u_avg += user_item_hash(user)(item)
        }

        var numerator, active_den, other_u_den = 0.0
        for(item <- co_items) {
          numerator += (user_item_hash(active_user)(item) - active_u_avg) * (user_item_hash(user)(item) - other_u_avg)
          active_den += math.pow(user_item_hash(active_user)(item) - active_u_avg, 2)
          other_u_den += math.pow(user_item_hash(user)(item) - other_u_avg, 2)
        }

        result(user) = numerator/(math.sqrt(active_den * other_u_den))
      }
    }

    return (result, co_rated_items)
  }

  def predictRating(active_user: String, item: String, weights: Map[String, Double],
                    user_item_hash: Map[String, Map[String, Double]], item_avg_hash: Map[String, (Set[String], Double)],
                    co_rated_items: Map[String, Set[String]], user_avg: Double): Double = {
    // Fetch out the users who have voted for the item and have some co-rated items with active user
    val users_set: Set[String] = item_avg_hash(item)._1.intersect(co_rated_items.keySet)

    if(users_set.size>0){
      // Pearson's weighted sum code
      var numerator: Double = 0.0
      var denominator: Double = 0.0

      for(user: String <- users_set) {
        val items: Set[String] = co_rated_items(user)
        var co_avg = 0.0
        for(i <- items) {
          co_avg += user_item_hash(user)(i)
        }

        // Average for that user
        co_avg /= items.size

        numerator += (user_item_hash(user)(item) - co_avg) * weights(user)
        denominator += math.abs(weights(user))
      }
      return (user_avg + (numerator/denominator)) * (math.log(user_item_hash.size) - math.log(item_avg_hash(item)._1.size))
    } else {
      return (user_avg + item_avg_hash(item)._2)/2
    }

  }

  def formatOutput(result: RDD[((String, String), Double)]): String = {
    val od = new Ordering[(String, String, Double)] {
      def compare(here: (String, String, Double), there: (String, String, Double)): Int = {
        var result = 0
        if ((here._1 compare there._1) != 0)
          result = here._1 compare there._1 //first string ascending

        else
          result = here._2 compare there._2 //second string ascending
        result
      }
    }

    val temp = result.map(x => (x._1._1, x._1._2, x._2)).sortBy(x => x, true, 1)(od, classTag[(String, String, Double)]).collect()
    var data = ""

    for(row: (String, String, Double) <- temp) {
      data += "\"" + row._1 + "\",\"" + row._2 + "\",\"" + row._3 + "\"\n"
    }

    return data
  }
}
