package com.mayurb.dwp.transform.model

import com.mayurb.spark.sql.dsl.DFFunc2
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


/**
  *
  * @author mbadgujar
  */
package object io {

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


  @JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
  @JsonSubTypes(Array(
    new Type(value = classOf[HiveTarget], name = "hive"),
    new Type(value = classOf[OrcTarget], name = "orc"),
    new Type(value = classOf[ParquetTarget], name = "parquet"),
    new Type(value = classOf[CSVTarget], name = "csv"),
    new Type(value = classOf[TextTarget], name = "text")
  ))
  abstract class TargetTypeRoot(@JsonProperty(value = "type", required = true) id: String, mode: String) {
    def apply(df: DataFrame): Unit

    def getSaveMode: SaveMode = mode match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "error_if_exits" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
    }
  }

  case class HiveTarget(@JsonProperty(required = true) mode: String,
                        @JsonProperty(required = true) db: String,
                        @JsonProperty(required = true) table: String) extends TargetTypeRoot("hive", mode) {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode).saveAsTable(s"$db.$table")
  }

  case class OrcTarget(@JsonProperty(required = true) mode: String,
                       @JsonProperty(required = true) path: String,
                       options: Map[String, String]) extends TargetTypeRoot("orc", mode) {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode).options(options).orc(path)
  }

  case class ParquetTarget(@JsonProperty(required = true) mode: String,
                           @JsonProperty(required = true) path: String,
                           options: Map[String, String]) extends TargetTypeRoot("parquet", mode) {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode).options(options).parquet(path)
  }

  case class CSVTarget(@JsonProperty(required = true) mode: String,
                       @JsonProperty(required = true) path: String,
                       options: Map[String, String]) extends TargetTypeRoot("csv", mode) {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode).options(options).csv(path)
  }

  case class TextTarget(@JsonProperty(required = true) mode: String,
                        @JsonProperty(required = true) path: String,
                        options: Map[String, String]) extends TargetTypeRoot("text", mode) {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode).options(options).text(path)
  }

}
