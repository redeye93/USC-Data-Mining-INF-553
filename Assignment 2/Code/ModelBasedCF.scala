import java.nio.file.{Files, Paths, Path}
import java.io.File

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import math.sqrt
import scala.reflect.classTag
import scala.collection.mutable.{ArrayBuffer, Map, HashMap}

object ModelBasedCF {
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

    // Create a temporary folder
    val tmp = Paths.get(".")
    val tmpdir = Files.createTempDirectory(tmp, null)

    // Set spark checkpoint
    sc.setCheckpointDir(tmpdir.toString)

    val rdd_train = getData(sc, args(0))

    // Create maps of user ids and business ids
    val (usr_map: Map[String, Int], bus_map: Map[String, Int], user_list: ArrayBuffer[String],
    bus_list: ArrayBuffer[String]) = generateMap(rdd_train)

    // Convert the business and user ids to integers
    val ratings_train = getRatingObject(rdd_train, usr_map, bus_map)

    // Build the recommendation model using ALS
    val rank = 2
    val numIterations = 40
    val model = ALS.train(ratings_train, rank, numIterations, 0.28)

    /**
      * Prediction part
      */
    val rdd_test = getData(sc, args(1))

    // Update the list with missing user and business ids
    for(row: Array[String] <- rdd_test.collect()){
      if(!usr_map.contains(row(0))) {
        user_list += row(0)
        usr_map(row(0)) = (user_list.length-1)
      }

      if(!bus_map.contains(row(1))) {
        bus_list += row(1)
        bus_map(row(1)) = (bus_list.length-1)
      }
    }

    // Convert the business and user ids to integers
    val ratings_test = rdd_test.map((x: Array[String]) =>
      Rating(usr_map(x(0)), bus_map(x(1)), x(2).toDouble)
    )

    /**
      * Eval phase
      */

    // Evaluate the model on test data, so first get the test data
    val usersProducts = ratings_test.map { case Rating(user, product, rate) =>
      (user, product)
    }

    // Predict the ratings
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    val temp: String = convertWrite(predictions, user_list, bus_list)

    /**
      * Write the output file
      */
    writeFile(temp, "ModelBasedCF", ".")

    val ratesAndPreds = rdd_test.map((x: Array[String]) =>
      ((usr_map(x(0)), bus_map(x(1))), x(2).toDouble)).join(predictions)

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
    val RMSE = sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean())
    println(s"RMSE: $RMSE")
    println("Time: " + ((System.nanoTime()-start_time)/1000000000 ) + " sec")
    deleteRecursively(new File(tmpdir.toString))
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)

    if (file.exists && !file.delete)
      return
  }

  def getData(sc: SparkContext, filePath: String): (RDD[Array[String]]) = {
    // Load and parse the data
    val data = sc.textFile(filePath)

    val lines = data.map(_.split(',')).collect().drop(1)
    val ratings = sc.parallelize(lines)

    return ratings
  }

  def generateMap(rdd_survey: RDD[Array[String]]): (Map[String, Int], Map[String, Int], ArrayBuffer[String],
    ArrayBuffer[String]) = {
    // Pick up unique user ids and calculate the sum and the total number of the entries and the user vector determinant
    val userIdList = rdd_survey.map((x: Array[String]) =>
      (x(0),(x(2).toFloat, 1))
    ).reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2)).collect()

    // Pick up unique business ids and calculate the sum and the total number of the entries
    val busiIdList: Array[(String, (Float, Int))] = rdd_survey.map((x: Array[String]) =>
      (x(1),(x(2).toFloat, 1))
    ).reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2)).collect()

    // Creation of a user Map and a user list for user string to integer mapping
    val userMap: HashMap[String, Int] = HashMap()
    var userList: ArrayBuffer[String] = ArrayBuffer()
    for( index <- 0 to userIdList.length-1){
      userMap(userIdList(index)._1) = index
      userList += userIdList(index)._1
    }

    val busiMap : HashMap[String, Int] = HashMap()
    var busiList: ArrayBuffer[String] = ArrayBuffer()
    for( index <- 0 to busiIdList.length-1){
      busiMap(busiIdList(index)._1) = index
      busiList += busiIdList(index)._1
    }

    return (userMap, busiMap, userList, busiList)
  }

  def getRatingObject(rdd_survey: RDD[Array[String]], userMap: Map[String, Int], busiMap: Map[String, Int]): RDD[Rating]= {
    return rdd_survey.map((x: Array[String]) =>
      Rating(userMap(x(0)), busiMap(x(1)), x(2).toDouble)
    )
  }

  def convertWrite(result: RDD[((Int, Int), Double)], usr_list: ArrayBuffer[String], bus_list: ArrayBuffer[String]): String = {
    val converted = result.map(x => (usr_list(x._1._1), bus_list(x._1._2), x._2))

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

    val temp = converted.sortBy(x => x, true, 1)(od, classTag[(String, String, Double)]).collect()
    var data = ""

    for(row: (String, String, Double) <- temp) {
      data += "\"" + row._1 + "\",\"" + row._2 + "\",\"" + row._3 + "\"\n"
    }

    return data
  }

  def writeFile(data: String, task: String, filePath: String): Unit= {
    import java.io.FileWriter
    val writer = new FileWriter(filePath + "/Utkarsh_Gera_" + task + ".txt")
    writer.write(data)
    writer.close()
  }
}
