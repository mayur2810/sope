package com.mayurb.dwp.transform.model.io

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Package contains YAML Transformer Output construct mappings and definitions
  *
  * @author mbadgujar
  */
package object output {


  @JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
  @JsonSubTypes(Array(
    new Type(value = classOf[HiveTarget], name = "hive"),
    new Type(value = classOf[OrcTarget], name = "orc"),
    new Type(value = classOf[ParquetTarget], name = "parquet"),
    new Type(value = classOf[CSVTarget], name = "csv"),
    new Type(value = classOf[TextTarget], name = "text"),
    new Type(value = classOf[JsonTarget], name = "text")
  ))
  abstract class TargetTypeRoot(@JsonProperty(value = "type", required = true) id: String, input: String, mode: String) {
    def apply(df: DataFrame): Unit

    def getSaveMode: SaveMode = mode match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "error_if_exits" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
    }

    def getInput: String = input

    def getOptions(options: Map[String, String]): Map[String, String] = Option(options).getOrElse(Map())
  }

  case class HiveTarget(@JsonProperty(required = true) input: String,
                        @JsonProperty(required = true) mode: String,
                        @JsonProperty(required = true) db: String,
                        @JsonProperty(required = true) table: String) extends TargetTypeRoot("hive", input, mode) {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode).saveAsTable(s"$db.$table")
  }

  case class OrcTarget(@JsonProperty(required = true) input: String,
                       @JsonProperty(required = true) mode: String,
                       @JsonProperty(required = true) path: String,
                       options: Map[String, String]) extends TargetTypeRoot("orc", input, mode) {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode).options(getOptions(options)).orc(path)
  }

  case class ParquetTarget(@JsonProperty(required = true) input: String,
                           @JsonProperty(required = true) mode: String,
                           @JsonProperty(required = true) path: String,
                           options: Map[String, String]) extends TargetTypeRoot("parquet", input, mode) {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode).options(getOptions(options)).parquet(path)
  }

  case class CSVTarget(@JsonProperty(required = true) input: String,
                       @JsonProperty(required = true) mode: String,
                       @JsonProperty(required = true) path: String,
                       options: Map[String, String]) extends TargetTypeRoot("csv", input, mode) {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode).options(getOptions(options)).csv(path)
  }

  case class TextTarget(@JsonProperty(required = true) input: String,
                        @JsonProperty(required = true) mode: String,
                        @JsonProperty(required = true) path: String,
                        options: Map[String, String]) extends TargetTypeRoot("text", input, mode) {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode).options(getOptions(options)).text(path)
  }

  case class JsonTarget(@JsonProperty(required = true) input: String,
                        @JsonProperty(required = true) mode: String,
                        @JsonProperty(required = true) path: String,
                        options: Map[String, String]) extends TargetTypeRoot("json", input, mode) {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode).options(getOptions(options)).json(path)
  }

}
