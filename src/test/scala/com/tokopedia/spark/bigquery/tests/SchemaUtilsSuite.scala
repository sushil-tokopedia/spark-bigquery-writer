package com.tokopedia.spark.bigquery.tests

import com.google.api.services.bigquery.model.TableRow
import com.google.cloud.bigquery.StandardSQLTypeName
import com.tokopedia.spark.bigquery.SchemaUtils
import org.scalatest.flatspec.AnyFlatSpec

class SchemaUtilsSuite extends AnyFlatSpec{

  def fixtures = {
    new {
      val simpleRow = new TableRow()
      simpleRow.put("field1", "value1")
      simpleRow.put("field2", "value2")

      val recordTypeRow = new TableRow()
      recordTypeRow.put("field1", "value1")
      val recordRow = new TableRow()
      recordRow.put("nested1", "nested-value1")
      recordTypeRow.put("nested", recordRow)
    }
  }


  "SchemaUtils" should "convert TableRow to BigQuery schema" in {
    val row = fixtures.simpleRow
    val schema = SchemaUtils.convertTableRowToBQSchema(row)
    assert(schema.getFields.size() == 2)
  }

  it should "create STRUCT type for nested fields" in {
    val row = fixtures.recordTypeRow
    val schema = SchemaUtils.convertTableRowToBQSchema(row)
    val field = schema.getFields.get("nested")
    assert(schema.getFields.size() == 2)
    assert(field.getType.getStandardType == StandardSQLTypeName.STRUCT)
  }

}
