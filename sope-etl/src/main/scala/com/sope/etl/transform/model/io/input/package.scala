package com.sope.etl.transform.model.io

import java.util.Properties

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.sope.spark.sql.DFFunc2
import com.sope.etl.yaml.{ParallelizeYaml, SchemaYaml}
import com.sope.spark.utils.google.BigQueryReader
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrameReader, SQLContext}

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
    new Type(value = classOf[CSVSource], name = "csv"),
    new Type(value = classOf[TextSource], name = "text"),
    new Type(value = classOf[JsonSource], name = "json"),
    new Type(value = classOf[JDBCSource], name = "jdbc"),
    new Type(value = classOf[BigQuerySource], name = "bigquery"),
    new Type(value = classOf[CustomSource], name = "custom"),
    new Type(value = classOf[LocalSource], name = "local")
  ))
  abstract class SourceTypeRoot(@JsonProperty(value = "type", required = true) id: String,
                                alias: String,
                                options: Option[Map[String, String]],
                                isStreaming: Option[Boolean] = Some(false),
                                schemaFile: Option[String] = None) {
    def apply: DFFunc2

    def getSourceName: String = alias

    protected def getSchema: Option[StructType] = schemaFile.fold(None: Option[StructType]) {
      file => Some(SchemaYaml(file).getSparkSchema)
    }

    def getReader(sqlContext: SQLContext): Either[DataFrameReader, DataStreamReader] =
      if (isStreaming.getOrElse(false)) {
        val streamReader = sqlContext.readStream.options(options.getOrElse(Map()))
        Right(getSchema.fold(streamReader)(schema => streamReader.schema(schema)))
      }
      else {
        val reader = sqlContext.read.options(options.getOrElse(Map()))
        Left(getSchema.fold(reader)(schema => reader.schema(schema)))
      }
  }

  /*
     Hive Source
   */
  case class HiveSource(@JsonProperty(required = true) alias: String,
                        @JsonProperty(required = true) db: String,
                        @JsonProperty(required = true) table: String) extends SourceTypeRoot("hive", alias, None) {
    def apply: DFFunc2 = (sqlContext: SQLContext) => sqlContext.table(s"$db.$table")
  }

  /*
   ORC Source
   */
  case class OrcSource(@JsonProperty(required = true) alias: String,
                       @JsonProperty(required = true) path: String,
                       @JsonProperty(value = "is_streaming") isStreaming: Option[Boolean],
                       @JsonProperty(value = "schema_file") schemaFile: Option[String],
                       options: Option[Map[String, String]])
    extends SourceTypeRoot("orc", alias, options, isStreaming, schemaFile) {
    def apply: DFFunc2 = (sqlContext: SQLContext) => getReader(sqlContext).fold(_.orc(path), _.orc(path))
  }

  /*
   Parquet Source
   */
  case class ParquetSource(@JsonProperty(required = true) alias: String,
                           @JsonProperty(required = true) path: String,
                           @JsonProperty(value = "is_streaming") isStreaming: Option[Boolean],
                           @JsonProperty(value = "schema_file") schemaFile: Option[String],
                           options: Option[Map[String, String]])
    extends SourceTypeRoot("parquet", alias, options, isStreaming, schemaFile) {
    def apply: DFFunc2 = (sqlContext: SQLContext) => getReader(sqlContext).fold(_.parquet(path), _.parquet(path))
  }

  /*
   CSV Source
   */
  case class CSVSource(@JsonProperty(required = true) alias: String,
                       @JsonProperty(required = true) path: String,
                       @JsonProperty(value = "is_streaming") isStreaming: Option[Boolean],
                       @JsonProperty(value = "schema_file") schemaFile: Option[String],
                       options: Option[Map[String, String]])
    extends SourceTypeRoot("csv", alias, options, isStreaming, schemaFile) {
    def apply: DFFunc2 = (sqlContext: SQLContext) => getReader(sqlContext).fold(_.csv(path), _.csv(path))
  }

  /*
   Text Source
   */
  case class TextSource(@JsonProperty(required = true) alias: String,
                        @JsonProperty(required = true) path: String,
                        @JsonProperty(value = "is_streaming") isStreaming: Option[Boolean],
                        @JsonProperty(value = "schema_file") schemaFile: Option[String],
                        options: Option[Map[String, String]])
    extends SourceTypeRoot("text", alias, options, isStreaming, schemaFile) {
    def apply: DFFunc2 = (sqlContext: SQLContext) => getReader(sqlContext).fold(_.text(path), _.text(path))
  }

  /*
   JSON Source
   */
  case class JsonSource(@JsonProperty(required = true) alias: String,
                        @JsonProperty(required = true) path: String,
                        @JsonProperty(value = "is_streaming") isStreaming: Option[Boolean],
                        @JsonProperty(value = "schema_file") schemaFile: Option[String],
                        options: Option[Map[String, String]])
    extends SourceTypeRoot("json", alias, options, isStreaming, schemaFile) {
    def apply: DFFunc2 = (sqlContext: SQLContext) => getReader(sqlContext).fold(_.json(path), _.json(path))
  }

  /*
   JDBC Source
   */
  case class JDBCSource(@JsonProperty(required = true) alias: String,
                        @JsonProperty(required = true) url: String,
                        @JsonProperty(required = true) table: String,
                        options: Option[Map[String, String]])
    extends SourceTypeRoot("jdbc", alias, options) {
    private val properties = options.fold(new Properties())(options => {
      val properties = new Properties()
      options.foreach { case (k, v) => properties.setProperty(k, v) }
      properties
    })

    def apply: DFFunc2 = (sqlContext: SQLContext) => sqlContext.read.jdbc(url, table, properties)
  }


  /*
   Big Query Source
   */
  case class BigQuerySource(@JsonProperty(required = true) alias: String,
                            @JsonProperty(required = true) db: String,
                            @JsonProperty(required = true) table: String,
                            @JsonProperty(value = "project_id") projectId: Option[String])
    extends SourceTypeRoot("bigquery", alias, None) {
    private val bqDataset = projectId.fold(s"$db.$table")(id => s"$id:$db.$table")

    def apply: DFFunc2 = (sqlContext: SQLContext) => new BigQueryReader(sqlContext, bqDataset).load()
  }


  /*
   Local Source
 */
  case class LocalSource(@JsonProperty(required = true) alias: String,
                         @JsonProperty(value = "yaml_file", required = true) yamlFile: String)
    extends SourceTypeRoot("local", alias, None, None, None) {
    def apply: DFFunc2 = (sqlContext: SQLContext) => ParallelizeYaml(yamlFile).parallelize(sqlContext)
  }


  /*
   Custom Source
   */
  case class CustomSource(@JsonProperty(required = true) alias: String,
                          @JsonProperty(required = true) format: String,
                          @JsonProperty(value = "is_streaming") isStreaming: Option[Boolean],
                          @JsonProperty(value = "schema_file") schemaFile: Option[String],
                          @JsonProperty(required = true) options: Option[Map[String, String]])
    extends SourceTypeRoot("custom", alias, options, isStreaming, schemaFile) {
    def apply: DFFunc2 = (sqlContext: SQLContext) => getReader(sqlContext).fold(_.format(format).load(), _.format(format).load())
  }

}
