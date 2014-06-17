import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

case class BenchConfig(
    @transient queriesObj: TpcDsQueries,
    @transient tablesObj: TpcDsTables,
    numIterPerQuery: Int,
    numWarmUpRuns: Int,
    dropOutlierPerc: Double)


object TpcDsBench extends App {

  // TODO: think about output location (output case class -> able to be processed by Spark SQL)

  def setup(args: Array[String]): (BenchConfig, SparkContext, HiveContext) = {
    if (args.size < 4) {
      println(
        """
          |Usage:
          |  <sparkMaster> <queries> [numIterPerQuery = 1] [numWarmUpRuns = 0] [dropOutlierPerc = 0.0]
          |
          |Example:
          |  local[4] q19,q53,ss_max 10 1 0.4
        """.stripMargin)
      println("Using default arguments for local developments...")
    }

    val sparkMaster = if (args.length > 1) args(0) else "local[4]"
    val queries = if (args.length > 2) args(1).split(",").toSeq else Seq("q0")
    val numIterPerQuery = if (args.length > 3) args(2).toInt else 1
    val numWarmUpRuns = if (args.length > 4) args(3).toInt else 0
    val dropOutlierPerc = if (args.length > 5) args(4).toDouble else 0.0

    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("TpcDsBench")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    val queriesObj = new TpcDsQueries(hc, queries) // TODO: locations?
    val tablesObj = new TpcDsTables(hc)

    val benchConfig = BenchConfig(
      queriesObj,
      tablesObj,
      numIterPerQuery,
      numWarmUpRuns,
      dropOutlierPerc
    )

    (benchConfig, sc, hc)
  }

  def setupTables(benchConfig: BenchConfig) = {
    benchConfig.tablesObj.allTables.foreach(_.collect())
  }

  def runWarmUp(benchConfig: BenchConfig) = {
    (1 to benchConfig.numWarmUpRuns).foreach { _ =>
      benchConfig.queriesObj.warmUpQuery.collect()
    }
  }

  override def main(args: Array[String]) {
    val (benchConfig, sc, hc) = setup(args)

    // NOTE: DDL commands should have been evaluated eagerly in setup() already.
    // setupTables(benchConfig)
    runWarmUp(benchConfig)

    // TODO: (1) run queries (2) report results

    sc.stop()
  }

}
