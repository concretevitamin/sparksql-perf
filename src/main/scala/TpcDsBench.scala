import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

case class TpcDsBenchConfig(
    @transient queriesObj: TpcDsQueries,
    @transient tablesObj: TpcDsTables,
    numIterPerQuery: Int,
    numWarmUpRuns: Int,
    dropOutlierPerc: Double) {
  override def toString = {
    val warm = s"Number of warm-up runs (before all queries, not each): $numWarmUpRuns"
    val iter = s"Number of iterations per query: $numIterPerQuery"
    val outlier = s"Outlier drop ratio: $dropOutlierPerc"
    Seq(warm, iter, outlier).mkString("\n")
  }
}

object TpcDsBench extends App with BenchmarkUtils {

  // TODO: think about output location (output case class -> able to be processed by Spark SQL)

  // TODO: how to take a conf (for hints)?

  private def setup(args: Array[String]): (TpcDsBenchConfig, SparkContext, HiveContext) = {
    if (args.size < 1) {
      println(
        """
          |Usage:
          |  <sparkMaster> [queries] [numIterPerQuery = 1] [numWarmUpRuns = 1] [dropOutlierPerc = 0.0]
          |
          |Example:
          |  local[4] q19,q53,ss_max 10 1 0.4
        """.stripMargin)
      sys.exit()
    }

    val sparkMaster = if (args.length > 0) args(0) else "local[4]"
    val queries = if (args.length > 1) args(1).split(",").toSeq else Seq()

    val numIterPerQuery = if (args.length > 2) args(2).toInt else 1
    val numWarmUpRuns = if (args.length > 3) args(3).toInt else 1
    val dropOutlierPerc = if (args.length > 4) args(4).toDouble else 0.0

    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("TpcDsBench")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    val queriesObj = new TpcDsQueries(hc, queries, System.getenv("TPCDS_DATA_DIR"))
    val tablesObj = new TpcDsTables(hc, System.getenv("TPCDS_DATA_DIR"))

    val benchConfig = TpcDsBenchConfig(
      queriesObj,
      tablesObj,
      numIterPerQuery,
      numWarmUpRuns,
      dropOutlierPerc
    )

    (benchConfig, sc, hc)
  }

  def setupTables(benchConfig: TpcDsBenchConfig) = {
    benchConfig.tablesObj.allTables.foreach(_.collect())
  }

  def runWarmUp(benchConfig: TpcDsBenchConfig) = {
    for (i <- 1 to benchConfig.numWarmUpRuns) {
      benchConfig.queriesObj.warmUpQuery.collect().foreach(println)
    }
  }

  def runQueries(benchConfig: TpcDsBenchConfig): Seq[BenchmarkResult] = {
    val numIter = benchConfig.numIterPerQuery
    val dropOutlierPerc = benchConfig.dropOutlierPerc

    val benchmark = runNumItersInMills(numIter)   andThen
                    dropOutliers(dropOutlierPerc) andThen
                    average

    benchConfig.queriesObj.allQueries.map { case (queryName, query) =>
      BenchmarkResult(queryName, benchmark { query.collect() })
    }
  }

  def printBenchmarkResults(conf: TpcDsBenchConfig, results: Seq[BenchmarkResult]) = {
    val res = results.map(_.toString).mkString("\n")
    val giant =
      s"""
         |******** benchmark config
         |$conf
         |
         |******** benchmark results
         |$res
       """.stripMargin
    println()
    println(giant)
    println()
  }

  override def main(args: Array[String]) {
    val (benchConfig, sc, hc) = setup(args)

    runWarmUp(benchConfig)

    val benchmarkResults = runQueries(benchConfig)
    printBenchmarkResults(benchConfig, benchmarkResults)

    sc.stop()
  }

}
