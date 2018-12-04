import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import util.control.Breaks.{breakable, break}

import scala.collection.mutable.{ListBuffer}

object Utkarsh_Gera_SON {
  var support_threshold = 0
  var num_of_partition = 0.toLong

  def main(args: Array[String]): Unit = {
    /**
      * Pre-setting
      */
    try {
      if( args.length!=3 ) {
        print("Need at three arguments - Input file, support threshold and output file")
        return
      } else {
        if ( !Files.exists(Paths.get(args(0))) ) {
          print("Input file doesn't exist")
          return
        }
        else if ( !Files.exists(Paths.get(args(2)).getParent) ) {
          print("Output folder doesn't exist")
          return
        }
      }
    } catch {
      case e: Exception => print("Need at three arguments - Input file, support threshold and output file")
    }

    // Record time
    var start_time = System.nanoTime()

    // Create spark configuration
    val config = new SparkConf().setAppName("SON").setMaster("local")
    val sc: SparkContext = new SparkContext(config)

    // Read the text File
    val rdd_train = sc.textFile(args(0))

    // Basket and item tuples from the rdd
    val baskets: RDD[(String, Set[String])] = rdd_train.map{row =>
      val content = row.split(",")
      (content(0), Set(content(1)))}.reduceByKey((a,b) => a++b)

    // Get the support and the bucket count
    support_threshold = args(1).toInt
    num_of_partition = baskets.getNumPartitions

    // Pass 1
    val potential_frequent_itemsets = baskets.mapPartitions(potentialFrequentItemSets).
      map(row => (row, 1)).reduceByKey((a,b) => 1).map(row => row._1).collect()

    // Pass 2
    val frequent_item_mixture: RDD[(Set[String], Int)] = baskets.mapPartitions{
      baskets: Iterator[(String, Set[String])] =>
      // Result format so that it can be reduced by the key
      var results: Map[Set[String], Int] = Map()

      for(bucket: (String, Set[String]) <- baskets) {
        for(item_set:  Set[String] <- potential_frequent_itemsets) {
          if(item_set.subsetOf(bucket._2)) {
            results += (item_set -> (results.getOrElse(item_set, 0) + 1))
          }
        }
      }

      results.toIterator
    }

    // Finalized categorized item sets
    val categorised_item_sets: Array[(Int, Iterable[String])] = frequent_item_mixture.reduceByKey(_ + _).
      filter(_._2>=support_threshold).map(row => (row._1.size, "(" + row._1.toList.sorted.mkString(", ") + ")")).
      groupByKey().sortByKey().collect()

    // Write the output
    writeFile(categorised_item_sets, args(2))

    println((System.nanoTime()-start_time)/1000000000)
  }

  // Find all the frequent item sets in that chunk
  def potentialFrequentItemSets(chunk : Iterator[(String, Set[String])]) : Iterator[Set[String]] = {
    // Result to be returned
    var result: ListBuffer[Set[String]] = ListBuffer()

    // Convert iterator to list
    var chunk_list = chunk.toList

    // Support threshold for this chunk
    val threshold = math.floor(support_threshold / num_of_partition)

    // Map for the singletons
    var singletons: Map[String, Set[String]] = chunk_list.map{row =>
      row._2.map(x => (x, row._1))
    }.flatten.groupBy(_._1).filter(row => row._2.length>=threshold).map{ case(k, v) =>
      result += Set(k)
      (k, v.map(_._2).toSet)}

    // Singleton items
    var previous = singletons.keySet

    // Start with combos
    var size = 2

    // Generate all other combinations
    while(previous.size >= size) {
      // Find all the subsets and initialize the next set of candidates
      val candidates: Iterator[Set[String]] = previous.subsets(size)
      var next_candidates: Set[String] = Set()

      for(c <- candidates) {
        val items: List[String] = c.toList
        var common_buckets: Set[String] = singletons(items(0))

        // for first item
        if(common_buckets.size >= threshold){
          var flag = true
          breakable{
            for(item <- items.slice(1, items.length)) {
              common_buckets = common_buckets.intersect(singletons(item))
              if(common_buckets.size < threshold) {
                flag = false
                break
              }
            }

            // Add to the result list
            if(flag) {
              result += c
              next_candidates = next_candidates ++ c
            }
          }
        }
      }

      previous = next_candidates
      size += 1
    }

    result.toIterator
  }

  def writeFile(results: Array[(Int, Iterable[String])], output_file: String): Unit= {
    import java.io.FileWriter
    val writer = new FileWriter(output_file)
    for(row <- results) {
      writer.write(row._2.toList.sorted.mkString(", ") + "\n\n")
    }
    writer.close()
  }
}
