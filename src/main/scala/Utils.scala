object Utils {

  def runWithTimingInMillis[A](f: => A): (Long, A) = {
    val start = System.nanoTime()
    val res = f
    val end = System.nanoTime()
    (((end - start) / 1e6).toLong, res)
  }

  def runWithTimingInSeconds[A](f: => A): (Long, A) = {
    val (millis, res) = runWithTimingInMillis(f)
    ((millis / 1e3).toLong, res)
  }

}
