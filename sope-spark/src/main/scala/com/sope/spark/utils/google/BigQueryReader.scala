package com.sope.spark.utils.google

import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, GsonBigQueryInputFormat}
import com.google.gson.JsonObject
import com.sope.common.utils.Logging
import org.apache.hadoop.io.LongWritable
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Big Query Reader
  *
  * @param sqlContext    Spark [[SQLContext]]
  * @param sourceBQTable Fully Qualified Big Query Table Name
  */
class BigQueryReader(sqlContext: SQLContext, sourceBQTable: String) extends Logging {

  private val sc = sqlContext.sparkContext
  private val conf = sc.hadoopConfiguration
  private val projectId = conf.get("fs.gs.project.id")
  private val bucket = conf.get("fs.gs.system.bucket")

  // Input configuration.
  conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
  conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucket)
  log.info("GCP Project ID: {}", projectId)
  log.info("GCP Bucket for temporary storage: {} ", bucket)
  BigQueryConfiguration.configureBigQueryInput(conf, sourceBQTable)

  /**
    * Load BQ Table
    */
  def load(): DataFrame = {
    import sqlContext.implicits._
    // Load data from BigQuery.
    val tableData = sc.newAPIHadoopRDD(
      conf,
      classOf[GsonBigQueryInputFormat],
      classOf[LongWritable],
      classOf[JsonObject])
      .map(_._2.toString)
    sqlContext.read.json(tableData.toDS)
  }
}
