package com.tokopedia.spark.bigquery

import java.util.concurrent.ConcurrentHashMap

import com.google.cloud.bigquery.{BigQuery, TableId}

object TableCache {
  private val lockVar = 10
  private val tableMap: ConcurrentHashMap[TableId, Boolean] = new ConcurrentHashMap[TableId, Boolean]()

  def exists(tableId: TableId): Boolean = {
    lockVar.synchronized {
      tableMap.getOrDefault(tableId, false)
    }
  }

  def addTable(tableId: TableId) = {
    tableMap.putIfAbsent(tableId, true)
  }
}
