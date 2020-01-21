Spark BigQuery Writer
=====================

Its a thin wrapper around BigQuery Client Java library which enables streaming writes to BigQuery tables of Spark DStreams.

Its quite lightweight with just one dependency BigQuery Java client.
```
libraryDependencies += "com.google.cloud" % "google-cloud-bigquery" % "1.103.0"
```

The main interface to interact with the wrapper is the `BigQueryWriter` object which can be used in following way

~~~Scala
BigQueryWriter.write[String](messages, toTableRow, toTableMetadata)

  def toTableRow(s: String): TableRow = {
    val num = s.toInt
    val row = new TableRow()
    row.put("number", num.asInstanceOf[Object]);
    row
  }

  def toTableMetadata(s: String): BigQueryTableMetadata = {
    val t = TableId.of("<DATASET>", "<TABLE-NAME>")
    BigQueryTableMetadata(t,SchemaUtils.convertTableRowToBQSchema(toTableRow(s)),false,Nil)
  }
~~~

The function `toTableMetadata` can be used to provide destination table on row basis. This design is inspired by Apache Beam's [DynamicDestinations](https://beam.apache.org/releases/javadoc/2.1.0/org/apache/beam/sdk/io/gcp/bigquery/DynamicDestinations.html).