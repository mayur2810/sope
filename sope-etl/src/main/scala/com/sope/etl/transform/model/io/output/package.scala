package com.sope.etl.transform.model.io

import java.util.Properties

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.sope.spark.sql._
import com.sope.utils.Logging
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
    new Type(value = classOf[JDBCTarget], name = "jdbc"),
    new Type(value = classOf[TextTarget], name = "text"),
    new Type(value = classOf[JsonTarget], name = "json"),
    new Type(value = classOf[CountOutput], name = "count"),
    new Type(value = classOf[ShowOutput], name = "show")
  ))
  abstract class TargetTypeRoot(@JsonProperty(value = "type", required = true) id: String)
    extends Logging {
    def apply(df: DataFrame): Unit

    def getSaveMode(mode: String): SaveMode = mode match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "error_if_exits" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
    }

    def getInput: String

    def getOptions(options: Map[String, String]): Map[String, String] = Option(options).getOrElse(Map())
  }

  case class HiveTarget(@JsonProperty(required = true) input: String,
                        @JsonProperty(required = true) mode: String,
                        @JsonProperty(required = true) db: String,
                        @JsonProperty(required = true) table: String,
                        @JsonProperty(required = false, value = "partition_by") partitionBy: Option[Seq[String]]) extends TargetTypeRoot("hive") {
    def apply(df: DataFrame): Unit = {
      val targetTable = s"$db.$table"
      val targetTableDF = df.sqlContext.table(targetTable)
      (partitionBy match {
        case Some(cols) =>
          df.select(targetTableDF.getColumns: _*).write.partitionBy(cols: _*)
        case None => df.select(targetTableDF.getColumns: _*).write
      }).mode(getSaveMode(mode)).insertInto(targetTable)
    }

    override def getInput: String = input
  }

  case class OrcTarget(@JsonProperty(required = true) input: String,
                       @JsonProperty(required = true) mode: String,
                       @JsonProperty(required = true) path: String,
                       options: Map[String, String]) extends TargetTypeRoot("orc") {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode(mode)).options(getOptions(options)).orc(path)

    override def getInput: String = input
  }

  case class ParquetTarget(@JsonProperty(required = true) input: String,
                           @JsonProperty(required = true) mode: String,
                           @JsonProperty(required = true) path: String,
                           options: Map[String, String]) extends TargetTypeRoot("parquet") {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode(mode)).options(getOptions(options)).parquet(path)

    override def getInput: String = input
  }

  case class JDBCTarget(@JsonProperty(required = true) input: String,
                         @JsonProperty(required = true) mode: String,
                         @JsonProperty(required = true) url: String,
                         @JsonProperty(required = true) table: String,
                         options: Map[String, String]) extends TargetTypeRoot("csv") {
    private val properties = Option(options).fold(new Properties())(options => {
      val properties = new Properties()
      options.foreach { case (k, v) => properties.setProperty(k, v) }
      properties
    })

    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode(mode)).jdbc(url, table, properties)

    override def getInput: String = input
  }

  case class TextTarget(@JsonProperty(required = true) input: String,
                        @JsonProperty(required = true) mode: String,
                        @JsonProperty(required = true) path: String,
                        options: Map[String, String]) extends TargetTypeRoot("text") {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode(mode)).options(getOptions(options)).text(path)

    override def getInput: String = input
  }

  case class JsonTarget(@JsonProperty(required = true) input: String,
                        @JsonProperty(required = true) mode: String,
                        @JsonProperty(required = true) path: String,
                        options: Map[String, String]) extends TargetTypeRoot("json") {
    def apply(df: DataFrame): Unit = df.write.mode(getSaveMode(mode)).options(getOptions(options)).json(path)

    override def getInput: String = input
  }

  case class CountOutput(@JsonProperty(required = true) input: String) extends TargetTypeRoot("count") {
    def apply(df: DataFrame): Unit = logInfo(s"Count for transformation alias: $input :- ${df.count}")

    override def getInput: String = input
  }

  case class ShowOutput(@JsonProperty(required = true) input: String,
                        @JsonProperty(required = true) num_records: Int) extends TargetTypeRoot("show") {
    def apply(df: DataFrame): Unit = {
      logInfo(s"Showing sample rows for transformation alias: $input")
      if(num_records == 0) df.show else df.show(num_records)
    }

    override def getInput: String = input
  }

}
