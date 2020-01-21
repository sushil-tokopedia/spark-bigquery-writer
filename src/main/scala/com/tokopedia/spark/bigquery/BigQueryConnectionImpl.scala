package com.tokopedia.spark.bigquery

import java.util.UUID

import com.google.api.services.bigquery.model.TableRow
import com.google.cloud.bigquery._
import com.tokopedia.spark.bigquery.models.BigQueryTableMetadata

import scala.collection.JavaConverters._


class BigQueryConnectionImpl(index: Int) extends BigQueryConnection {

  private val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService
  private val connectionId: String = UUID.randomUUID().toString

  private implicit val self = this

  override def insert[T](records: Iterator[T], formatFunction: T => TableRow, tableName: T => BigQueryTableMetadata) = {
    records.map(row => (tableName(row), formatFunction(row))).toList
      .groupBy(_._1)
      .foreach(record => {
        val table = record._1
        val rows = record._2.map(r => r._2)

        if (!TableCache.exists(table.tableId)) {
          create(table)
          TableCache.addTable(table.tableId)
        }

        val request = InsertAllRequest.newBuilder(table.tableId)
        rows.foreach(r => request.addRow(r))
        bigquery.insertAll(request.build())
      })
  }

  override def create(tableMetadata: BigQueryTableMetadata): Unit = {
    if (checkIfTableExist(tableMetadata.tableId)) { // To ensure this function is idempotent
      return
    }

    val tableDef = StandardTableDefinition.of(tableMetadata.schema)
      .toBuilder

    if (tableMetadata.clusteringColumns.size > 0) {
      tableDef.setClustering(Clustering.newBuilder()
        .setFields(tableMetadata.clusteringColumns.asJava)
        .build())
    }

    /* Currently this only support Ingestion time partitioning.
       Will add support for custom time partitioning soon. */

    if (tableMetadata.isTimePartitioned) {
      tableDef.setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
    }

    val tableInfo = TableInfo.newBuilder(tableMetadata.tableId, tableDef.build()).build()
    bigquery.create(tableInfo)
  }

  private def checkIfTableExist(tableId: TableId): Boolean = {
    bigquery.getTable(tableId.getDataset, tableId.getTable) != null
  }

  override def toString: String = {
    s"BigQueryConnection@${connectionId}"
  }
}

