import java.nio.file.{Files, Paths}
import Task1.{calculationByCountry, getData, writeOutput}
import org.apache.spark.SparkConf

object Task3 {
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

    val config = new SparkConf().setAppName("Task3").setMaster("local[2]")
    val rdd_survey = getData(config, "Task3", args(0))._1.rdd

    val salary = rdd_survey.map(x => if(x(1).toString.equalsIgnoreCase("NA") ||
      x(1).toString.equalsIgnoreCase("0"))
      {("NA", (0, 0.0, 0.0, BigDecimal(0.0)) )}
    else { if(x(2).toString.equalsIgnoreCase("monthly")) {
        val sal = 12 * x(1).toString.replaceAll(",","").toDouble
        (x(0).toString, (1, sal, sal, BigDecimal(sal)))
      } else if(x(2).toString.equalsIgnoreCase("weekly")) {
        val sal = 52 * x(1).toString.replaceAll(",","").toDouble
        (x(0).toString, (1, sal, sal, BigDecimal(sal)))
      } else (x(0).toString, (1, x(1).toString.replaceAll(",","").toDouble,
      x(1).toString.replaceAll(",","").toDouble,
      BigDecimal(x(1).toString.replaceAll(",","").toDouble)))
    })

    val result = salary.reduceByKey((x , y) => (x._1 + y._1, math.min(x._2, y._2), math.max(x._3, y._3), x._4 + y._4)).collect().sortBy(_._1)
    var output = ""
    for(row <- result) {
      if (row._2._1 > 0)
        output += row._1 + "," + row._2._1 + "," + row._2._2.toInt + "," + row._2._3.toInt + "," + (row._2._4/BigDecimal(row._2._1)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString() + "\n"
    }

    writeOutput(output, "task3", args(1))
  }
}
