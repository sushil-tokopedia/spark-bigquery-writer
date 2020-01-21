package com.tokopedia.spark.bigquery

trait ConnectionPool {
  def getConnection(): BigQueryConnection
  def returnConnection(connection: BigQueryConnection)
}
