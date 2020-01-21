package com.tokopedia.spark.bigquery

import com.google.api.services.bigquery.model.TableRow
import com.tokopedia.spark.bigquery.models.BigQueryTableMetadata

trait BigQueryConnection {
  def insert[T](records: Iterator[T], formatFunction: T => TableRow, tableName: T => BigQueryTableMetadata)
  def create(tableMetadata: BigQueryTableMetadata): Unit
}
