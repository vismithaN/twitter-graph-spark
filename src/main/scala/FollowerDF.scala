import org.apache.spark.sql
import org.apache.spark.sql.{Encoders, SparkSession,functions => F}


import java.util.Base64.Encoder

object FollowerDF {

  /**
   * This function should first read the graph located at the input path, it should compute the
   * follower count, and save the top 100 users to the output path in parquet format.
   *
   * It must be done using the DataFrame/Dataset API.
   *
   * It is NOT valid to do it with the RDD API, and convert the result to a DataFrame, nor to read
   * the graph as an RDD and convert it to a DataFrame.
   *
   * @param inputPath  the path to the graph.
   * @param outputPath the output path.
   * @param spark      the spark session.
   */
  def computeFollowerCountDF(inputPath: String, outputPath: String, spark: SparkSession): Unit = {
    // TODO: Calculate the follower count for each user
    // TODO: Write the top 100 users to the above outputPath in parquet format
    implicit val tupleEncoder: sql.Encoder[(Long, Long)] = Encoders.tuple(Encoders.scalaLong, Encoders.scalaLong)
    val graphDF = spark.read.textFile(inputPath).map(line => {
      val parts = line.split("\t")
      (parts(0).toLong, parts(1).toLong)
    }).toDF("source", "destination")

    //    val followersDF = graphDF.groupBy("destination").count().alias("num_followers").orderBy("num_followers")

    val top100 = graphDF.groupBy("target_user")
      .agg(F.count("source_user").alias("num_followers")) // Use agg() and alias the count
      .orderBy(F.desc("num_followers")).limit(100)

    top100.write.parquet(outputPath)
  }
    /**
     * @param args it should be called with two arguments, the input path, and the output path.
     */
    def main(args: Array[String]): Unit = {
      val spark = SparkUtils.sparkSession()

      val inputGraph = args(0)
      val followerDFOutputPath = args(1)

      computeFollowerCountDF(inputGraph, followerDFOutputPath, spark)
    }


}
