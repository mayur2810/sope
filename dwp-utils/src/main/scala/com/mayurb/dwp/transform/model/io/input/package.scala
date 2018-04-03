package com.mayurb.dwp.transform.model.io

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.mayurb.spark.sql.DFFunc2
import org.apache.spark.sql.SQLContext

/**
  *
  * @author mbadgujar
  */
package object input {

  @JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
  @JsonSubTypes(Array(
    new Type(value = classOf[HiveSource], name = "hive"),
    new Type(value = classOf[OrcSource], name = "orc"),
    new Type(value = classOf[ParquetSource], name = "parquet"),
    new Type(value = classOf[CSVSource], name = "csv"),
    new Type(value = classOf[TextSource], name = "text")
  ))
  abstract class SourceTypeRoot(@JsonProperty(value = "type", required = true) id: String, alias: String) {
    def apply: DFFunc2

    def getSourceName: String = alias
  }

  case class HiveSource(@JsonProperty(required = true) alias: String,
                        @JsonProperty(required = true) db: String,
                        @JsonProperty(required = true) table: String) extends SourceTypeRoot("hive", alias) {
    def apply: DFFunc2 = (sqlContext: SQLContext) => sqlContext.table(s"$db.$table")
  }

  case class OrcSource(@JsonProperty(required = true) alias: String,
                       @JsonProperty(required = true) path: String,
                       options: Map[String, String]) extends SourceTypeRoot("orc", alias) {
    def apply: DFFunc2 = (sqlContext: SQLContext) => sqlContext.read.options(options).orc(path)
  }

  case class ParquetSource(@JsonProperty(required = true) alias: String,
                           @JsonProperty(required = true) path: String,
                           options: Map[String, String]) extends SourceTypeRoot("parquet", alias) {
    def apply: DFFunc2 = (sqlContext: SQLContext) => sqlContext.read.options(options).parquet(path)
  }

  case class CSVSource(@JsonProperty(required = true) name: String,
                       @JsonProperty(required = true) path: String,
                       options: Map[String, String]) extends SourceTypeRoot("csv", name) {
    def apply: DFFunc2 = (sqlContext: SQLContext) => sqlContext.read.options(options).csv(path)
  }

  case class TextSource(@JsonProperty(required = true) alias: String,
                        @JsonProperty(required = true) path: String,
                        options: Map[String, String]) extends SourceTypeRoot("text", alias) {
    def apply: DFFunc2 = (sqlContext: SQLContext) => sqlContext.read.options(options).text(path)
  }


}
