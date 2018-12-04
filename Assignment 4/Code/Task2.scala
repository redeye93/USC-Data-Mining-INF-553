import java.nio.file.{Files, Paths}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF, IDF, IDFModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering._
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import scala.collection.mutable.ListBuffer

object Task2 {
  var numClusters = 0
  var numIterations = 0
  val hashingTF = new HashingTF(numFeatures = 2^80)

  case class Cluster(
                      id: Int,
                      size: Int,
                      error: Double,
                      terms: List[String]
                    )
  case class Algo(
                   algorithm: String,
                   WSSE: Double,
                   clusters: List[Cluster]
                 )

  def main(args: Array[String]): Unit = {
    /**
      * Pre-setting
      */
    try {
      if( args.length!=4 ) {
        print("Need at four arguments - Input file, algorithm, number of clusters and number of iterations")
        return
      } else {
        if ( !Files.exists(Paths.get(args(0))) ) {
          print("Input file doesn't exist")
          return
        }
      }
    } catch {
      case e: Exception => print("Need at four arguments - Input file, algorithm, number of clusters and number of iterations")
    }

    // Pick up the values from the cmd line arguments
    numClusters = args(2).toInt
    numIterations = args(3).toInt

    // Create spark configuration
    val config = new SparkConf().setAppName("ModelBasedCF").setMaster("local")
    val sc: SparkContext = new SparkContext(config)

    // Load and parse the data
    val review_words: RDD[Seq[String]] = sc.textFile(args(0)).map(row => row.split(" ").toSeq)
    val (document_vectors: RDD[Vector], idf_Model: IDFModel) = preProcessing(review_words)

    // Output required
    var cluster_words: RDD[(Int, Iterable[(String, Int)])] = null
    var error_and_size: Array[(Int, (Double, Int))] = null
    var wsse: Double = 0.0
    var algo = ""

    // Start matching by the algorithm name
    args(1) match {
      case "K" => {
        val (model: KMeansModel, wssse) = kMeans(document_vectors)
        wsse = wssse
        val clustroid = model.clusterCenters

        // Calculate the error and the sizes
        error_and_size = document_vectors.map{ d_vector: Vector =>
          val cluster_id = model.predict(d_vector)
          (cluster_id, (Vectors.sqdist(d_vector, clustroid(cluster_id)), 1))
        }.reduceByKey((a, b) => (a._1+b._1,a._2+b._2)).collect()

        // Find out the words belonging to that cluster
        cluster_words = review_words.mapPartitions{document_list: Iterator[Seq[String]] =>
          val result: ListBuffer[((String, Int), Int)] = ListBuffer()

          for(document <- document_list) {
            val vector_tf: Vector = hashingTF.transform(document)
            val tfidf_vec: Vector = idf_Model.transform(vector_tf)

            val cluster_id = model.predict(tfidf_vec)

            for(word <- document) {
              result.append(((word, cluster_id), 1))
            }
            result
          }

          result.toIterator
        }.reduceByKey((a,b)=> a+b).map(row => (row._1._2,(row._1._1, row._2))).groupByKey()
        algo = "K-Means"
      }
      case "B" => {
        val (model:BisectingKMeansModel, wssse) = bkMeans(document_vectors)
        wsse = wssse
        val clustroid = model.clusterCenters

        // Calculate the error and the sizes
        error_and_size = document_vectors.map{ d_vector: Vector =>
          val cluster_id = model.predict(d_vector)
          (cluster_id, (Vectors.sqdist(d_vector, clustroid(cluster_id)), 1))
        }.reduceByKey((a, b) => (a._1+b._1, a._2+b._2)).collect()

        // Find out the words belonging to that cluster
        cluster_words = review_words.mapPartitions{document_list: Iterator[Seq[String]] =>
          val result: ListBuffer[((String, Int), Int)] = ListBuffer()

          for(document <- document_list) {
            val vector_tf: Vector = hashingTF.transform(document)
            val tfidf_vec: Vector = idf_Model.transform(vector_tf)

            val cluster_id = model.predict(tfidf_vec)

            for(word <- document) {
              result.append(((word, cluster_id), 1))
            }
            result
          }

          result.toIterator
        }.reduceByKey((a,b)=> a+b).map(row => (row._1._2,(row._1._1, row._2))).groupByKey()
        algo = "Bisecting K-Means"
      }
      /*case "G" => {
        val model = GMM(document_vectors)

        for (i <- 0 until model.k) {
          val mag_array =  model.gaussians(i).mu.toArray
          var mag = 0.0
          for(mag_dim <- mag_array) {
              mag += math.pow(mag_dim, 2)
          }

          println("weight=%f\nmean=%s\nsigma=\n%s\n" format
            (model.weights(i), math.sqrt(mag), model.gaussians(i).sigma))
        }

      }*/
      case _ => {
        println("Not a recognized algorithm. Exiting.")
        return
      }
    }

    // Sort the cluster words
    val clus_words: Array[(Int, Iterable[(String, Int)])] = cluster_words.sortByKey().collect()

    // Prepare the list of the top words
    val format_top_words: ListBuffer[ListBuffer[String]] = ListBuffer()
    for(words <- clus_words) {
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
    while(index<error_and_size.length) {
      clus_details += Cluster(index+1, error_and_size(index)._2._2, error_and_size(index)._2._1,
        format_top_words(index).toList)
      index += 1
    }

    val cluster_algo = Algo(algo, wsse, clus_details.toList)

    var dataset = "small"
    if(review_words.getNumPartitions>1) {
      dataset = "large"
    }
    writeFile(cluster_algo,"Utkarsh_Gera_Cluster_" + dataset + "_" + args(1) + "_" + numClusters + "_" + numIterations + ".json")
  }

  /**
    * To train KMeans model and get the WSSSE
    * @param document_vectors RDD of the reviews in vector format
    * @return the KMeans model and the WSSSE
    */
  def kMeans(document_vectors: RDD[Vector]): (KMeansModel, Double) ={
    val kmeans = new KMeans().setMaxIterations(numIterations).setSeed(42).setK(numClusters)
    // Cluster the data into two classes using KMeans
    val clusters = kmeans.run(document_vectors)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(document_vectors)
    println(WSSSE)

    return (clusters,WSSSE)
  }

  /**
    * To train the Bisecting KMeans model and get the WSSSE
    * @param document_vectors RDD of the reviews in the vector format
    * @return the Bisecting KMeans model and the WSSSE
    */
  def bkMeans(document_vectors: RDD[Vector]): (BisectingKMeansModel, Double) ={
    val bkm = new BisectingKMeans().setMaxIterations(numIterations).setSeed(42).setK(numClusters)
    val clusters = bkm.run(document_vectors)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(document_vectors)

    return (clusters,WSSSE)
  }

  /*def GMM(document_vectors: RDD[Vector]): GaussianMixtureModel ={
    // Cluster the data into two classes using GaussianMixture
    return new GaussianMixture().setMaxIterations(numIterations).setK(numClusters).run(document_vectors)
  }*/

  /**
    * To Generate TFIDF features from the dataset
    * @param documents RDD of sequence of words in the review
    * @return the RDD of tfidf vectors and the idf model
    */
  def preProcessing(documents: RDD[Seq[String]]): (RDD[Vector], IDFModel) ={
    val tf: RDD[Vector] = hashingTF.transform(documents)

    tf.cache()
    val idf = new IDF().fit(tf)
    return (idf.transform(tf).map(vec => vec.toDense), idf)
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
