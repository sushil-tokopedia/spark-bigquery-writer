package com.tokopedia.spark.bigquery

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

class BigQueryConnectionPool(val maxPoolSize: Int) extends ConnectionPool {

  private val connections: ConcurrentLinkedQueue[BigQueryConnection] = new ConcurrentLinkedQueue[BigQueryConnection]()
  private val counter = new AtomicInteger(0)

  override def getConnection(): BigQueryConnection = {

    if (counter.get() > maxPoolSize - 1) {
      throw new UnsupportedOperationException(s"Cannot use more than ${maxPoolSize} concurrent connections. Either return connections to pool or increase pool size.")
    }
    counter.incrementAndGet()
    connections.poll()
  }

  override def returnConnection(connection: BigQueryConnection): Unit = {
    counter.decrementAndGet()
    connections.add(connection)
  }
}


object BigQueryConnectionPool {
  private val DEFAULT_MAX_CONNECTIONS = 50

  def apply(): BigQueryConnectionPool = {
    apply(DEFAULT_MAX_CONNECTIONS)
  }

  def apply(poolSize: Int): BigQueryConnectionPool = {
    val pool = new BigQueryConnectionPool(poolSize)
    initPool(pool)
    pool
  }

  private def initPool(pool: BigQueryConnectionPool): Unit = {
    (0 to pool.maxPoolSize - 1) foreach { i => pool.connections.add(new BigQueryConnectionImpl(i)) }
  }


}
