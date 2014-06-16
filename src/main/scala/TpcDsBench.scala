import org.apache.spark.{SparkConf, SparkContext}

object TpcDsBench extends App {

  // TODO: think about output location (output case class -> able to be processed by Spark SQL)
  def setupSparkContext(args: Array[String]) = {
    if (args.size < 4) {
      sys.error(
        """
          |usage:           <sparkMaster> <queries> <numIterPerQuery> [ignoreOutliers = 0.0]
          |example:         local[4] q1,q53 10 true 0.4
        """.stripMargin)
    }

    val sparkMaster = if (args.length > 1) args(0) else "local[4]"
    val queries = if (args.length > 2) args(1).split(",") else Seq("q0")
    val numIterPerQuery = if (args.length > 3) args(2).toInt else 1
    val ignoreOutliers = if (args.length > 3) args(3).toDouble else 0.0

    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("TpcDsBench")
    (queries, new SparkContext(conf))
  }

  override def main(args: Array[String]) {
    val (queries, sc) = setupSparkContext(args)
  }

}
