package com.tokopedia.spark.bigquery

import com.google.api.services.bigquery.model.TableRow
import com.google.cloud.bigquery.{Field, Schema, StandardSQLTypeName}

import scala.collection.JavaConverters._

object SchemaUtils {

  implicit def convertTableRowToBQSchema(row: TableRow): Schema = {
    val fields = row.asScala map {
      case (key, value: String) => {
        Field.of(key, StandardSQLTypeName.STRING)
      }

      case (key, value: TableRow) => {
        Field.of(key, StandardSQLTypeName.STRUCT, getSubFields(value): _*)
      }
    }

    Schema.of(fields.asJava)
  }

  private def getSubFields(row: TableRow): Seq[Field] = {
    row.asScala map {
      case (key, value: String) => {
        Field.of(key, StandardSQLTypeName.STRING)
      }

      case (key, value: TableRow) => {
        Field.of(key, StandardSQLTypeName.STRUCT, getSubFields(value): _*)
      }
    } toList
  }
}
