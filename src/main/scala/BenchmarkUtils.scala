/**
 * Helper functions that are useful for benchmarking purposes.
 */
trait BenchmarkUtils {

  // TODO: other fields? e.g. detailed runtime breakdowns by types (computation vs. communication) or by stages.
  case class BenchmarkResult(benchmarkName: String, runTimeInMillis: Long) {
    override def toString = s"$benchmarkName runtime (millis): $runTimeInMillis"
  }

  def runWithTimingInMillis[A](desc: Any)(f: => A): (Long, A) = {
    val start = System.nanoTime()
    val res = f
    val end = System.nanoTime()
    (((end - start) / 1e6).toLong, res)
  }

  def runWithTimingInSeconds[A](desc: Any)(f: => A): (Long, A) = {
    val (millis, res) = runWithTimingInMillis(desc)(f)
    ((millis / 1e3).toLong, res)
  }


  def runNumItersInMills[A]: (Int) => (=> A) => Seq[Long] = {
    numIter => func =>
      (1 to numIter).map { _ =>
        val (runTime, _) = runWithTimingInMillis("run") { func }
        runTime
      }
  }

  /** Drops about dropOutlierPerc * nums.size outliers, half for the top and half for the bottom. */
  def dropOutliers(dropOutlierPerc: Double): Seq[Long] => Seq[Long] = {
    nums =>
      val cnt = (nums.size * dropOutlierPerc).toInt
      val top = cnt / 2
      val bottom = cnt - top
      nums.sorted.drop(bottom).dropRight(top)
  }

  def average(nums: Seq[Long]): Long = (nums.sum * 1.0 / nums.size).toLong

}
