import java.nio.file.{Files, Paths}

import Task2.{Algo, Cluster}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import scala.collection.mutable.ListBuffer
import scala.util.Random

object Task1 {
  var numClusters = 0
  var numIterations = 0
  val hashingTF = new HashingTF()

  def main(args: Array[String]): Unit = {
    /**
      * Pre-setting
      */
    try {
      if (args.length != 4) {
        print("Need at four arguments - Input file, feature, number of clusters and number of iterations")
        return
      } else {
        if (!Files.exists(Paths.get(args(0)))) {
          print("Input file doesn't exist")
          return
        }
      }
    } catch {
      case e: Exception => print("Need at four arguments - Input file, algorithm, number of clusters and number of iterations")
    }

    // Update the args
    numClusters = args(2).toInt
    numIterations = args(3).toInt

    // Create spark configuration
    val config = new SparkConf().setAppName("ModelBasedCF").setMaster("local")
    val sc: SparkContext = new SparkContext(config)

    // Load and parse the data
    val review_words: RDD[Seq[String]] = sc.textFile(args(0)).map(row => row.split(" ").toSeq)

    // Term frequency format
    val tf: RDD[Vector] = hashingTF.transform(review_words)

    // Final result of clusters to words with their count as iterable
    var cluster_words_iterable: RDD[(Int, Iterable[(String, Int)])] = null
    var wssse: Double = 0.0
    var error_sq_and_size: Array[(Int,(Double, Double, Int))] = null

    args(1) match {
      case "W" => {
        // Apply K Means
        val new_clustroids: Array[Vector] = k_means(tf)

        // Calculate the error and the sizes
        error_sq_and_size = tf.map{ d_vector: Vector =>
          val (cluster_id: Int, error: Double) = predict(d_vector, new_clustroids)
          (cluster_id, (error, math.pow(error, 2), 1))
        }.reduceByKey((a, b) => (a._1+b._1,a._2+b._2, a._3+b._3)).collect()

        //Top words in a cluster
        cluster_words_iterable = review_words.mapPartitions{document_list: Iterator[Seq[String]] =>
          val result: ListBuffer[((String, Int), Int)] = ListBuffer()

          // Find the clusters for the documents
          for(document <- document_list) {
            val vector_tf: Vector = hashingTF.transform(document)
            val (cluster_id: Int, _: Double) = predict(vector_tf, new_clustroids)

            // Emit word with cluster id and the count
            for(word <- document) {
              result.append(((word, cluster_id), 1))
            }
            result
          }

          result.toIterator
        }.reduceByKey((a,b)=> a+b).map(row => (row._1._2,(row._1._1, row._2))).groupByKey()
      }
      case "T" => {
        tf.cache()
        val idf = new IDF().fit(tf)
        val tfidf = idf.transform(tf)

        // Apply K Means
        val new_clustroids: Array[Vector] = k_means(tfidf)

        // Calculate the error and the sizes
        error_sq_and_size = tfidf.map{ d_vector: Vector =>
          val (cluster_id: Int, error: Double) = predict(d_vector, new_clustroids)
          (cluster_id, (error, math.pow(error, 2), 1))
        }.reduceByKey((a, b) => (a._1+b._1,a._2+b._2, a._3+b._3)).collect()

        //Top words in a cluster
        cluster_words_iterable = review_words.mapPartitions{document_list: Iterator[Seq[String]] =>
          val result: ListBuffer[((String, Int), Int)] = ListBuffer()

          // Find the clusters for the documents
          for(document <- document_list) {
            val vector_tfidf = idf.transform(hashingTF.transform(document))
            val (cluster_id: Int, _: Double) = predict(vector_tfidf, new_clustroids)

            // Emit word with cluster id and the count
            for(word <- document) {
              result.append(((word, cluster_id), 1))
            }
            result
          }

          result.toIterator
        }.reduceByKey((a,b)=> a+b).map(row => (row._1._2,(row._1._1, row._2))).groupByKey()
      }
    }

    // Sort the list of words in a cluster based on the cluster id and collect it
    val cluster_words: Array[(Int, Iterable[(String, Int)])] = cluster_words_iterable.sortByKey().collect()

    // Prepare the list of the top words
    val format_top_words: ListBuffer[ListBuffer[String]] = ListBuffer()
    for(words <- cluster_words) {
      // Sort and pick up the top 10 words by frequency
      val w_c = words._2.toList.sortBy(_._2).reverse.take(10)

      val temp: ListBuffer[String] = ListBuffer()
      for(w<-w_c) {
        temp += w._1
      }

      format_top_words += temp
    }

    // Make the final output
    val clus_details: ListBuffer[Cluster] = ListBuffer()

    // Prepare the details of each cluster
    var index = 0
    while(index<error_sq_and_size.length) {
      clus_details += Cluster(index+1, error_sq_and_size(index)._2._3, error_sq_and_size(index)._2._1,
        format_top_words(index).toList)
      wssse += error_sq_and_size(index)._2._2
      index += 1
    }

    val cluster_algo = Algo("K-Means", wssse, clus_details.toList)

    writeFile(cluster_algo,"Utkarsh_Gera_KMeans_small_" + args(1) + "_" + numClusters + "_" + numIterations + ".json")
  }

  /**
    * Generate k random clustroids as the starting points by picking up ransom k vectors from the dataset
    * @param review_vectors Review converted to tf vectors
    * @return Array of the randomly chosen clustroids
    */
  def generateRandomClustroids(review_vectors: RDD[Vector]): Array[Vector] = {
    val clusteroids: ListBuffer[Vector] = ListBuffer()

    val seed = Random
    seed.setSeed(20181031)

    // Collect the reviews
    val temp = review_vectors.collect()
    var index = 0

    while(index<numClusters) {
      // Select a random clustroid
      clusteroids += temp(seed.nextInt(review_vectors.count().toInt))
      index += 1
    }

    return clusteroids.toArray
  }

  /**
    * Generate the new clustroid centers
    * @param tf the vectors of the reviews
    * @param centroids The old centroids
    * @return Array of Vectors which are the new clustroids
    */
  def generateNewClusteroids(tf: RDD[Vector], centroids: Array[Vector]): Array[Vector] = {
    val clusters_vectors: RDD[(Int, Vector)] = tf.map{vector =>
      var min_index: Int = 0
      var min = Vectors.sqdist(vector, centroids(min_index))

      var index =1
      // Find the nearest centroid to the cluster
      while(index < centroids.length) {
        val temp = Vectors.sqdist(vector, centroids(index))
        if(temp<min) {
          min_index = index
          min = temp
        }
        index += 1
      }

      // Key with vector as a dense array and its count
      (min_index, vector)
    }

    var index = 0
    val new_clusters: ListBuffer[Vector] = ListBuffer()
    while(index < centroids.length) {
      val cluster_vectors: RDD[Vector] = clusters_vectors.filter(row => row._1 == index).map(row => row._2)
      val summary: MultivariateStatisticalSummary = Statistics.colStats(cluster_vectors)
      new_clusters += summary.mean

      index += 1
    }

    return new_clusters.toArray
  }

  /**
    * K Means algorithm
    * @param vectors for the reviews
    * @return the final set of clustroids
    */
  def k_means(vectors: RDD[Vector]): Array[Vector] = {
    var old_clustroids: Array[Vector] = null
    var new_clustroids: Array[Vector] = generateRandomClustroids(vectors)
    // Iterate n times to get decent clustroid vectors
    var iter = 0
    do {
      old_clustroids = new_clustroids
      new_clustroids = generateNewClusteroids(vectors, old_clustroids)
      iter += 1
    }while(iter<=numIterations)

    return new_clustroids
  }
  /**
    * Predicts the cluster id to which the vector belons
    * @param vector to find its suitable cluster
    * @param centroids  the possible centroid candidates
    * @return the centroid index
    */
  def predict(vector: Vector, centroids: Array[Vector]): (Int, Double) = {
    var min_index: Int = 0
    var min = Vectors.sqdist(vector, centroids(min_index))

    var index = 1
    while(index < centroids.length) {
      val temp = Vectors.sqdist(vector, centroids(index))
      if(temp<min) {
        min_index = index
        min = temp
      }
      index += 1
    }

    return (min_index, min)
  }

  /**
    * Write the results to a file
    * @param results the class to be serialized
    * @param output_file location of the output file
    */
  def writeFile(results: Algo, output_file: String): Unit= {
    import java.io.FileWriter
    implicit val formats = Serialization.formats(NoTypeHints)
    val writer = new FileWriter(output_file)
    writer.write(write(results))
    writer.close()
  }
}
