package com.sope.etl.transform.model.io

import java.util.Properties

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.sope.etl.transform.exception.YamlDataTransformException
import com.sope.spark.sql._
import com.sope.spark.utils.google.BigQueryWriter
import com.sope.utils.Logging
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

/**
  * Package contains YAML Transformer Output construct mappings and definitions
  *
  * @author mbadgujar
  */
package object output {

  case class BucketingOption(@JsonProperty(value = "num_buckets") numBuckets: Int = 200,
                             @JsonProperty(required = true) columns: Seq[String])

  case class TriggerOption(@JsonProperty(value = "trigger_type", required = true) triggerType: String, interval: String)


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
    new Type(value = classOf[JsonTarget], name = "json"),
    new Type(value = classOf[BigQueryTarget], name = "bigquery"),
    new Type(value = classOf[CustomTarget], name = "custom"),
    new Type(value = classOf[CountOutput], name = "count"),
    new Type(value = classOf[ShowOutput], name = "show")
  ))
  abstract class TargetTypeRoot(@JsonProperty(value = "type", required = true) id: String,
                                input: String,
                                mode: Option[String],
                                partitionBy: Option[Seq[String]],
                                bucketBy: Option[BucketingOption],
                                options: Option[Map[String, String]],
                                outputMode: Option[String] = None,
                                trigger: Option[TriggerOption] = None) extends Logging {

    def apply(df: DataFrame): Unit

    def getInput: String = input

    def getId: String = id

    def getWriter(df: DataFrame): DataFrameWriter[Row] = {
      val writeModeApplied = mode.fold(df.write)(_ => df.write.mode(mode.get))
      val partitioningApplied = partitionBy.fold(writeModeApplied)(cols => writeModeApplied.partitionBy(cols: _*))
      val bucketingApplied = bucketBy
        .fold(partitioningApplied)(bucketingOption => partitioningApplied
          .bucketBy(bucketingOption.numBuckets, bucketingOption.columns.head, bucketingOption.columns.tail: _*))
      bucketingApplied.options(options.getOrElse(Map.empty))
    }

    def getStreamWriter(df: DataFrame): DataStreamWriter[Row] = {
      val writeModeApplied = outputMode.fold(df.writeStream)(_ => df.writeStream.outputMode(outputMode.get))
      val partitioningApplied = partitionBy.fold(writeModeApplied)(cols => writeModeApplied.partitionBy(cols: _*))
      trigger.fold(partitioningApplied)(triggerOption => {
        val trigger = triggerOption.triggerType.toLowerCase match {
          case "processing_time" => Trigger.ProcessingTime(triggerOption.interval)
          case "once" => Trigger.Once()
          case "continuous" => Trigger.Continuous(triggerOption.interval)
          case _ => throw new YamlDataTransformException(s"invalid Trigger mode provided for streaming input: $input")
        }
        partitioningApplied.trigger(trigger)
      }).options(options.getOrElse(Map.empty))
    }
  }

  /*
     Hive Target
   */
  case class HiveTarget(@JsonProperty(required = true) input: String,
                        @JsonProperty(required = true) mode: Option[String],
                        @JsonProperty(required = true) db: String,
                        @JsonProperty(required = true) table: String,
                        @JsonProperty(value = "save_as_table") saveAsTable: Option[Boolean],
                        @JsonProperty(value = "partition_by") partitionBy: Option[Seq[String]],
                        @JsonProperty(value = "bucket_by") bucketBy: Option[BucketingOption],
                        options: Option[Map[String, String]])
    extends TargetTypeRoot("hive", input, mode, partitionBy, bucketBy, options) {
    def apply(df: DataFrame): Unit = {
      val targetTable = s"$db.$table"
      val saveAsTableFlag = saveAsTable.getOrElse(false)
      if (saveAsTableFlag)
        getWriter(df).saveAsTable(targetTable)
      else {
        val targetTableDF = df.sqlContext.table(targetTable)
        // reorder the columns as per table schema before inserting into table
        getWriter(df.select(targetTableDF.getColumns: _*)).insertInto(targetTable)
      }
    }
  }

  /*
    ORC Target
  */
  case class OrcTarget(@JsonProperty(required = true) input: String,
                       @JsonProperty(required = true) mode: Option[String],
                       @JsonProperty(required = true) path: String,
                       @JsonProperty(value = "partition_by") partitionBy: Option[Seq[String]],
                       @JsonProperty(value = "bucket_by") bucketBy: Option[BucketingOption],
                       options: Option[Map[String, String]])
    extends TargetTypeRoot("orc", input, mode, partitionBy, bucketBy, options) {
    def apply(df: DataFrame): Unit = getWriter(df).orc(path)
  }

  /*
    Parquet Target
  */
  case class ParquetTarget(@JsonProperty(required = true) input: String,
                           @JsonProperty(required = true) mode: Option[String],
                           @JsonProperty(required = true) path: String,
                           @JsonProperty(value = "partition_by") partitionBy: Option[Seq[String]],
                           @JsonProperty(value = "bucket_by") bucketBy: Option[BucketingOption],
                           options: Option[Map[String, String]])
    extends TargetTypeRoot("parquet", input, mode, partitionBy, bucketBy, options) {
    def apply(df: DataFrame): Unit = getWriter(df).parquet(path)
  }

  /*
    CSV Target
  */
  case class CSVTarget(@JsonProperty(required = true) input: String,
                       @JsonProperty(required = true) mode: Option[String],
                       @JsonProperty(required = true) path: String,
                       @JsonProperty(value = "partition_by") partitionBy: Option[Seq[String]],
                       @JsonProperty(value = "bucket_by") bucketBy: Option[BucketingOption],
                       options: Option[Map[String, String]])
    extends TargetTypeRoot("csv", input, mode, partitionBy, bucketBy, options) {
    def apply(df: DataFrame): Unit = getWriter(df).csv(path)
  }

  /*
    Text Target
  */
  case class TextTarget(@JsonProperty(required = true) input: String,
                        @JsonProperty(required = true) mode: Option[String],
                        @JsonProperty(required = true) path: String,
                        @JsonProperty(value = "partition_by") partitionBy: Option[Seq[String]],
                        @JsonProperty(value = "bucket_by") bucketBy: Option[BucketingOption],
                        options: Option[Map[String, String]])
    extends TargetTypeRoot("text", input, mode, partitionBy, bucketBy, options) {
    def apply(df: DataFrame): Unit = getWriter(df).text(path)
  }

  /*
    JSON Target
  */
  case class JsonTarget(@JsonProperty(required = true) input: String,
                        @JsonProperty(required = true) mode: Option[String],
                        @JsonProperty(required = true) path: String,
                        @JsonProperty(value = "partition_by") partitionBy: Option[Seq[String]],
                        @JsonProperty(value = "bucket_by") bucketBy: Option[BucketingOption],
                        options: Option[Map[String, String]])
    extends TargetTypeRoot("json", input, mode, partitionBy, bucketBy, options) {
    def apply(df: DataFrame): Unit = getWriter(df).json(path)
  }

  /*
    JDBC Target
  */
  case class JDBCTarget(@JsonProperty(required = true) input: String,
                        @JsonProperty(required = true) mode: Option[String],
                        @JsonProperty(required = true) url: String,
                        @JsonProperty(required = true) table: String,
                        options: Option[Map[String, String]])
    extends TargetTypeRoot("jdbc", input, mode, None, None, options) {
    private val properties = options.fold(new Properties())(options => {
      val properties = new Properties()
      options.foreach { case (k, v) => properties.setProperty(k, v) }
      properties
    })

    def apply(df: DataFrame): Unit = getWriter(df).jdbc(url, table, properties)

    override def getInput: String = input
  }

  /*
    Show Count
  */
  case class CountOutput(@JsonProperty(required = true) input: String)
    extends TargetTypeRoot("count", input, None, None, None, None) {
    def apply(df: DataFrame): Unit = logInfo(s"Count for transformation alias: $input :- ${df.count}")
  }

  /*
    Show sample result
  */
  case class ShowOutput(@JsonProperty(required = true) input: String, num_records: Int)
    extends TargetTypeRoot("show", input, None, None, None, None) {
    def apply(df: DataFrame): Unit = {
      logInfo(s"Showing sample rows for transformation alias: $input")
      if (num_records == 0) df.show(num_records, truncate = false) else df.show(false)
    }
  }

  /*
   BigQuery Target
  */
  case class BigQueryTarget(@JsonProperty(required = true) input: String,
                            @JsonProperty(required = true) db: String,
                            @JsonProperty(required = true) table: String,
                            @JsonProperty(value = "project_id") projectId: Option[String],
                            @JsonProperty(required = true) mode: Option[String])
    extends TargetTypeRoot("custom", input, mode, None, None, None, None) {
    private val bqDataset = projectId.fold(s"$db.$table")(id => s"$id:$db.$table")
    private val overwriteFlag = mode.fold(false)(mode => if (mode.toLowerCase == "overwrite") true else false)

    def apply(df: DataFrame): Unit = new BigQueryWriter(df, bqDataset, overwriteFlag).save()
  }

  /*
    Custom target
  */
  case class CustomTarget(@JsonProperty(required = true) input: String,
                          @JsonProperty(required = true) format: String,
                          mode: Option[String],
                          @JsonProperty(value = "partition_by") partitionBy: Option[Seq[String]],
                          @JsonProperty(value = "bucket_by") bucketBy: Option[BucketingOption],
                          @JsonProperty(value = "is_streaming") isStreaming: Option[Boolean],
                          @JsonProperty(value = "output_mode") outputMode: Option[String],
                          options: Option[Map[String, String]])
    extends TargetTypeRoot("custom", input, mode, partitionBy, bucketBy, options, outputMode) {
    def apply(df: DataFrame): Unit =
      if (isStreaming.getOrElse(false))
        getStreamWriter(df).format(format).start()
      else
        getWriter(df).format(format).save()
  }

}
