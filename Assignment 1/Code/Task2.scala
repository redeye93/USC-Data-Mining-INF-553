import java.nio.file.{Files, Paths}
import Task1.{calculationByCountry, getData, writeOutput}
import org.apache.spark.SparkConf

object Task2 {
  def main(args: Array[String]): Unit = {
    if( args.size!=2 ) {
      print("Need at two arguments - Input file and output folder path")
      return
    } else {
      if ( !Files.exists(Paths.get(args(0))) ) {
        print("Input file doesn't exist")
        return
      }
      else if ( !Files.exists(Paths.get(args(1))) ) {
        print("Output folder doesn't exist")
        return
      }
    }

    // Create spark configuration
    val config = new SparkConf().setAppName("Task2").setMaster("local[2]")
    var output = "standard,"
    var time = "standard,"

    val (df_survey, spark) = getData(config, "Task2", args(0))

    var std_rdd = df_survey.rdd
    if (std_rdd.getNumPartitions>2) {
      std_rdd = std_rdd.repartition(2)
    }

    // Put the size of the standard
    var size_rdd = std_rdd.mapPartitions(iter => Array(iter.size).iterator, true)
    var sizes = size_rdd.collect()
    output += sizes(0) + "," + sizes(1) + "\n"

    // Time for the standard calculation
    time += calculationByCountry(std_rdd)._2 + "\n"

    // This import is needed to use the $-notation
    import spark.implicits._

    // Repartitioning the data as per statement
    val rdd_survey = df_survey.repartition(2, $"Country").rdd

    // Size calculation for partition
    size_rdd = rdd_survey.mapPartitions(iter => Array(iter.size).iterator, true)
    sizes = size_rdd.collect()
    output += "partition," + sizes(0) + "," + sizes(1)

    // Time for the partition calculation
    time += "partition," + calculationByCountry(std_rdd)._2

    writeOutput(output, "task2", args(1))
    writeOutput(time, "task2_time", args(1))

  }
}
