package com.sope.etl.transform.model.io

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.sope.spark.sql.DFFunc2
import org.apache.spark.sql.SQLContext

/**
  * Package contains YAML Transformer Input construct mappings and definitions
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
    new Type(value = classOf[TextSource], name = "text"),
    new Type(value = classOf[JsonSource], name = "json")
  ))
  abstract class SourceTypeRoot(@JsonProperty(value = "type", required = true) id: String) {
    def apply: DFFunc2

    def getSourceName: String

    def getOptions(options: Map[String, String]): Map[String, String] = Option(options).getOrElse(Map())
  }

  case class HiveSource(@JsonProperty(required = true) alias: String,
                        @JsonProperty(required = true) db: String,
                        @JsonProperty(required = true) table: String) extends SourceTypeRoot("hive") {
    def apply: DFFunc2 = (sqlContext: SQLContext) => sqlContext.table(s"$db.$table")

    override def getSourceName: String = alias
  }

  case class OrcSource(@JsonProperty(required = true) alias: String,
                       @JsonProperty(required = true) path: String,
                       options: Map[String, String]) extends SourceTypeRoot("orc") {
    def apply: DFFunc2 = (sqlContext: SQLContext) => sqlContext.read.options(getOptions(options)).orc(path)

    override def getSourceName: String = alias
  }

  case class ParquetSource(@JsonProperty(required = true) alias: String,
                           @JsonProperty(required = true) path: String,
                           options: Map[String, String]) extends SourceTypeRoot("parquet") {
    def apply: DFFunc2 = (sqlContext: SQLContext) => sqlContext.read.options(getOptions(options)).parquet(path)

    override def getSourceName: String = alias
  }

/*  case class CSVSource(@JsonProperty(required = true) name: String,
                       @JsonProperty(required = true) path: String,
                       options: Map[String, String]) extends SourceTypeRoot("csv", name) {
    def apply: DFFunc2 = (sqlContext: SQLContext) => sqlContext.read.options(getOptions(options)).csv(path)
  }*/

  case class TextSource(@JsonProperty(required = true) alias: String,
                        @JsonProperty(required = true) path: String,
                        options: Map[String, String]) extends SourceTypeRoot("text") {
    def apply: DFFunc2 = (sqlContext: SQLContext) => sqlContext.read.options(getOptions(options)).text(path)
    override def getSourceName: String = alias
  }

  case class JsonSource(@JsonProperty(required = true) alias: String,
                        @JsonProperty(required = true) path: String,
                        options: Map[String, String]) extends SourceTypeRoot("json") {
    def apply: DFFunc2 = (sqlContext: SQLContext) => sqlContext.read.options(getOptions(options)).json(path)
    override def getSourceName: String = alias
  }

}
