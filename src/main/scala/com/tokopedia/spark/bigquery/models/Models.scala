package com.tokopedia.spark.bigquery.models

import com.google.cloud.bigquery.{Schema, TableId}

case class BigQueryTableMetadata(tableId: TableId, schema: Schema, isTimePartitioned: Boolean, clusteringColumns: Seq[String])
