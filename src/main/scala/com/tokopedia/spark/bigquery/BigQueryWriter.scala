package com.tokopedia.spark.bigquery

import com.google.api.services.bigquery.model.TableRow
import com.tokopedia.spark.bigquery.models.BigQueryTableMetadata
import org.apache.spark.streaming.dstream.DStream


object BigQueryWriter {

  def write[T](stream: DStream[T], f: T => TableRow, g: T => BigQueryTableMetadata) = {
    stream.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        // Runs on worker... Create connection pool here
        val pool = BigQueryConnectionPool()
        val connection = pool.getConnection()
        connection.insert[T](partitionOfRecords, f, g)
        pool.returnConnection(connection)
      })
    }
    )
  }
}
