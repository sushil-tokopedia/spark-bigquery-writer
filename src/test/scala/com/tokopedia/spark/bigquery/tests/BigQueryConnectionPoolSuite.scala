package com.tokopedia.spark.bigquery.tests

import com.tokopedia.spark.bigquery.BigQueryConnectionPool
import org.scalatest.flatspec.AnyFlatSpec


class BigQueryConnectionPoolSuite extends AnyFlatSpec {

  def fixture = {
    new {
      val pool = BigQueryConnectionPool(1)
      val defaultPool = BigQueryConnectionPool()
    }
  }

  "A connection pool" should "allow to get connections" in {
    val p = fixture.pool
    val connection = p.getConnection()
    assert(connection != null)
  }

  it should  "have default size of 50" in {
    val defaultPool = fixture.defaultPool
    assert(defaultPool.maxPoolSize == 50)
  }

  it should "allow to return connection back to pool" in {
    val p = fixture.pool
    val connection = p.getConnection()
    p.returnConnection(connection)
  }

  it should "produce UnsupportedOperationException when getConnection() is invoked more than pool size." in {
  val p = fixture.pool
    p.getConnection()
    assertThrows[UnsupportedOperationException](p.getConnection())
  }
}
