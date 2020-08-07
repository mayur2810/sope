package com.sope.spark.utils.google

import com.google.cloud.hadoop.io.bigquery.output.{BigQueryOutputConfiguration, BigQueryTableFieldSchema, BigQueryTableSchema, IndirectBigQueryOutputFormat}
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryFileFormat}
import com.sope.common.utils.Logging
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

/**
  * Dataframe to BigQuery writer
  *
  * @author mbadgujar
  */
class BigQueryWriter(sourceDF: DataFrame, targetBQTable: String, overwriteTable: Boolean = false) extends Logging {

  import BigQueryWriter._

  private val sourceColumns = sourceDF.columns
  private val sqlContext = sourceDF.sqlContext
  private val hadoopConf = sqlContext.sparkContext.hadoopConfiguration

  /**
    * Get the Big Query Schema from Spark Dataframe
    *
    * @return [[BigQueryTableSchema]]
    */
  private def getBQSchema: BigQueryTableSchema = {
    val bqFields = sourceDF.dtypes.map { case (cname, ctype) =>
      val bqType = ctype.toUpperCase.replaceAll("TYPE", "") match {
        case "LONG" => "INTEGER"
        case decimal if decimal.startsWith("DECIMAL") => "FLOAT"
        case other => other
      }
      val bqField = new BigQueryTableFieldSchema()
      bqField.setName(cname)
      bqField.setType(bqType)
      bqField
    }.toList
    val bqTable = new BigQueryTableSchema()
    bqTable.setFields(bqFields)
  }

  /**
    * Save BQ Table
    */
  def save(): Unit = {
    val projectId = hadoopConf.get("fs.gs.project.id")
    val bucket = hadoopConf.get("fs.gs.system.bucket")
    log.info("GCP Project ID: {}", projectId)
    log.info("GCP Bucket for temporary storage: {} ", bucket)
    val outputGcsPath = s"gs://$bucket/hadoop/tmp/bigquery/$targetBQTable"
    log.info("GCP Path for temporary storage: {} ", outputGcsPath)
    hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    hadoopConf.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucket)
    hadoopConf.set("mapreduce.job.outputformat.class", classOf[IndirectBigQueryOutputFormat[_, _]].getName)
    if (overwriteTable)
      hadoopConf.set(BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY, "WRITE_TRUNCATE")

    BigQueryOutputConfiguration.configure(
      hadoopConf,
      targetBQTable,
      getBQSchema,
      outputGcsPath,
      BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
      classOf[TextOutputFormat[_, _]])

    val jsonDF = sourceDF.withColumn("json_data",
      to_json(struct(sourceColumns.map(col): _*))).select(JsonColumn)

    jsonDF.rdd
      .map(row => (null, row.getAs[String](0)))
      .saveAsNewAPIHadoopDataset(hadoopConf)
  }
}

object BigQueryWriter {
  private val JsonColumn = "json_data"
}
