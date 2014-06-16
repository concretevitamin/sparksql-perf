import org.scalatest.FunSuite

import org.apache.spark.sql.hive.test.TestHive

class TpcDsSuite extends FunSuite {

  test("parse TPC-DS tables and queries") {
    new TpcDsTables(TestHive)
    new TpcDsQueries(TestHive)
  }

//  test("parse TPC-DS table commands") {
//  }

}
