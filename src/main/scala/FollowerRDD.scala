import org.apache.spark.SparkContext

object FollowerRDD {

  /**
    * This function should first read the graph located at the input path, it should compute the
    * follower count, and save the top 100 users to the output path with userID and
    * count **tab separated**.
    *
    * It must be done using the RDD API.
    *
    * @param inputPath the path to the graph.
    * @param outputPath the output path.
    * @param sc the SparkContext.
    */
  def computeFollowerCountRDD(inputPath: String, outputPath: String, sc: SparkContext): Unit = {
    // TODO: Calculate the follower count for each user
    // TODO: Write the top 100 users to the outputPath with userID and count **tab separated**
    val graphRDD = sc.textFile(inputPath)
    val edgesRDD = graphRDD.map(line => {
      val parts = line.split("\t")
      (parts(0).toLong, parts(1).toLong)
    })
    val followersRDD = edgesRDD.map { case (u, v) => (v, 1) }.reduceByKey(_ + _).sortBy(_._2, ascending = false)
    val top100 = followersRDD.take(100)
    sc.parallelize(top100).map{ case (userId, numFollowers) => s"$userId\t$numFollowers" }.saveAsTextFile(outputPath)
  }

  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()
    val sc = spark.sparkContext

    val inputGraph = args(0)
    val followerRDDOutputPath = args(1)

    computeFollowerCountRDD(inputGraph, followerRDDOutputPath, sc)
  }
}
