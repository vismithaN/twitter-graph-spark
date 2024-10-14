import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}


object PageRank {

  // Do not modify
  val PageRankIterations = 10

  /**
    * Input graph is a plain text file of the following format:
    *
    *   follower  followee
    *   follower  followee
    *   follower  followee
    *   ...
    *
    * where the follower and followee are separated by `\t`.
    *
    * After calculating the page ranks of all the nodes in the graph,
    * the output should be written to `outputPath` in the following format:
    *
    *   node  rank
    *   node  rank
    *   node  rank
    *
    * where node and rank are separated by `\t`.
    *
    * @param inputGraphPath path of the input graph.
    * @param outputPath path of the output of page rank.
    * @param iterations number of iterations to run on the PageRank.
    * @param spark the SparkSession.
    */
  def calculatePageRank(
      inputGraphPath: String,
      graphTopicsPath: String,
      pageRankOutputPath: String,
      recsOutputPath: String,
      iterations: Int,
      spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    import spark.implicits._


    // load graph
    val schema = StructType(Array(
      StructField("follower", LongType),
      StructField("followee", LongType)
    ))

    val df = spark.read
      .option("sep", "\t")
      .schema(schema)
      .csv(inputGraphPath)

    // load topics
    val schema2 = StructType(Array(
      StructField("user", LongType),
      StructField("games", DoubleType),
      StructField("movies", DoubleType),
      StructField("music", DoubleType)
    ))

    val df_topics = spark.read
      .option("sep", "\t")
      .schema(schema2)
      .csv(graphTopicsPath)

    // collect static data
    val df_following = df.groupBy("follower")
      .agg(collect_list("followee") as "followees")
      .withColumn("numfollowing", size(col("followees")))

    val statics = df_following.join(df_topics, df_following("follower") === df_topics("user"), "right")

    val totalUsers = df.select("follower")
      .union(df.select("followee"))
      .distinct()
      .count()

//    // collect variable data
//    val vertices = df.flatMap(r => Array((r.getLong(0), 0.0), (r.getLong(1), 0.0)))
//      .toDF("user", "rank")
//    var ranks = vertices.groupBy("user").agg(lit(1.0/totalUsers).as("rank"))

    var vars = df_topics
      // TODO: Note that you will have to change the rank initialization based on the information given in the writeup
      .withColumn("rank", lit(1.0/totalUsers))
      .select("user", "rank")

    for (i <- 1 to 10) {
      val joined = statics.join(vars, "user")

      // explode each row in joined df into contribution vectors for each followee
      val contribs = joined
        .withColumn("followee", explode_outer(concat($"followees", array($"user"))))
        .withColumn("contrib", when($"user" !== $"followee", joined("rank") / joined("numfollowing"))
          .otherwise(0))
        .select($"user", coalesce($"followee", $"user").alias("followee"), $"contrib")
        .na.fill(0, Seq("contrib"))

      // TODO: Handle dangling node contributions
      val danglingNodes = joined.filter(size($"followees") === 0)
      val danglingRankSum = if (danglingNodes.count() == 0) {
        0.0 // No dangling nodes, so the sum is 0.0
      } else {
        // Sum the ranks of dangling nodes if they exist
        danglingNodes
          .agg(sum($"rank").cast("double").as("dangling_rank_sum"))
          .select("dangling_rank_sum")
          .first()
          .getDouble(0)
      }
      val danglingContribution = danglingRankSum / totalUsers

      // aggregate contributions
      vars = contribs.groupBy("followee").agg(
          sum("contrib").as("rank"))
        .withColumn("rank", ($"rank" + lit(danglingContribution)) * lit(0.85) + lit(0.15))
        .withColumnRenamed("followee", "user")
    }

    // get final ranks
    val rank_output = vars.select("user", "rank")
    // TODO: Write to the output files
    rank_output.write.option("sep", "\t").option("header", "false").save(pageRankOutputPath)

  }


  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()

    val inputGraph = args(0)
    val graphTopics = args(1)
    val pageRankOutputPath = args(2)
    val recsOutputPath = args(3)

    calculatePageRank(inputGraph, graphTopics, pageRankOutputPath, recsOutputPath, PageRankIterations, spark)
  }
}
