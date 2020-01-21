package com.tokopedia.spark.bigquery.tests

import com.google.cloud.bigquery.TableId
import com.tokopedia.spark.bigquery.TableCache
import org.scalatest.flatspec.AnyFlatSpec

class TableCacheSuite extends  AnyFlatSpec{

  def fixtures = {
    new {
      val existingTable = TableId.of("dataset","existing_table")
      val nonExistingTable = TableId.of("dataset","non_existing_table")
    }
  }


  "TableCache" should "allow to add tables" in {
    val table = fixtures.existingTable
    assert(!TableCache.addTable(table)) // addTable returns false (the previous associated value with table)
  }

  it should "return true when exists() is called for existing table" in {
    val table = fixtures.existingTable
    assert(TableCache.exists(table))
  }

  it should "return false when exists() is called for non-existing table" in {
    val table = fixtures.nonExistingTable
    assert(!TableCache.exists(table))
  }

}
