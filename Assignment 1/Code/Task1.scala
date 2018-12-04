import java.nio.file.{Files, Paths}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Task1 {
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
    val config = new SparkConf().setAppName("Task1").setMaster("local[2]")

    val rdd_survey = getData(config, "Task1", args(0))._1.rdd

    // On discussion with TA Prasad
    if(rdd_survey.getNumPartitions > 2) {
      rdd_survey.repartition(2)()
    }

    var output = "Total,"
    // Total calculation
    output += totalCalculation(rdd_survey).toString + "\n"

    // Only pick up the result and not time
    val countrySorted = calculationByCountry(rdd_survey)._1

    //Output format creation
    for(arr <- countrySorted) {
      if(arr._2>0) {
        output += arr._1 + "," + arr._2 + "\n"
      }
    }

    writeOutput(output, "task1", args(1))
  }

  def totalCalculation(rdd_survey: RDD[Row]): Int = {
    val total = rdd_survey.map{x=> if(x(1).toString=="NA" || x(1).toString=="0"){0} else {1}}
    val result = total.reduce((a,b) => a+b)
    return result
  }

  def calculationByCountry(rdd_survey: RDD[Row]): (Array[(String, Int)], Long) = {
    val keyValue = rdd_survey.map(x=> if(x(1).toString=="NA" || x(1).toString=="0"){(x(0).toString, 0)} else {(x(0).toString,1)})

    // That it will perform the action
    //keyValue.collect()
    //    208040693
    //    17741576

    //    10674319
    //    10241860

    // Time calculation
    val st = System.nanoTime()
    val result = keyValue.reduceByKey((a,b)=> a+b)
    val ed = System.nanoTime()

    return (result.collect().sortBy(_._1), ed-st)
  }

  def writeOutput(data: String, task: String, filePath: String): Unit= {
    import java.io.FileWriter
    val writer = new FileWriter(filePath + "/Utkarsh_Gera_" + task + ".csv")
    writer.write(data)
    writer.close()
  }

  def getData(config: SparkConf, appName: String, filePath: String): (DataFrame, SparkSession) = {

    val spark = SparkSession
    .builder()
    .appName(appName)
    .config(config)
    .getOrCreate()

    val sc = spark.sparkContext

    // This import is needed to use the $-notation
    import spark.implicits._

    val survey = spark.read.option("header", true).csv(filePath)
    val m_survey = survey.select($"Country", $"Salary",$"SalaryType")
    return (m_survey, spark)
  }
}
