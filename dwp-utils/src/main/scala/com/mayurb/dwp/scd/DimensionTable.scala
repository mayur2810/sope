package com.mayurb.dwp.scd

import com.mayurb.utils.Logging
import org.apache.spark.SparkException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Dimension table class to support Dimension load process.
  *
  * @param dimensionDF    [[DataFrame]] Dimension table dataframe
  * @param surrogateKey   [[String]] Surrogate key column name
  * @param naturalKeys    [[Seq[String]] List of natural key columns for the Dimension
  * @param derivedColumns [[Seq[String]] List of derived columns for the Dimension. Derived columns will not be considered for SCD
  *                       For update records, If source has the derived columns then source value will be considered else target values
  * @param metaColumns    [[Seq[String]] List of meta columns for the Dimension table. Meta columns will not be considered for SCD
  *                       Meta columns will be null for the insert and update records
  */
class DimensionTable(dimensionDF: DataFrame,
                     surrogateKey: String,
                     naturalKeys: Seq[String],
                     derivedColumns: Seq[String] = Seq(),
                     metaColumns: Seq[String] = Seq()) extends Logging {

  import DimensionTable._
  import dimensionDF.sqlContext.implicits._


  // Columns to skip for MD5 calculation
  private val columnsToSkip = Seq(surrogateKey) ++ derivedColumns ++ metaColumns

  private def validate(dataFrame: DataFrame, columns: Seq[String]): Boolean = {
    columns.forall(dataFrame.columns.contains(_))
  }

  // Validate if dimension dataframe has provided columns
  if (!validate(dimensionDF, columnsToSkip ++ naturalKeys))
    throw new SparkException("Provided Dimension Dataframe does not contain the surrogate key/natural keys/derived/meta columns")

  // create key and non-key column list
  private val (keyColumns, nonKeyColumns) = dimensionDF.columns.toSeq
    .filterNot(columnsToSkip.contains(_))
    .map(new Column(_))
    .partition(column => naturalKeys.contains(column.toString))

  logInfo(s"Dimension table Key columns:- ${keyColumns.mkString(", ")}")
  logInfo(s"Dimension table Non-Key columns:- ${nonKeyColumns.mkString(", ")}")


  /**
    * Get Column list according to 'Source' or 'Target' table alias
    *
    * @param from             'Source' or 'Target'
    * @param sourceColumnList Source Columns, required to check the derived colums
    * @return [[Column]]s to Select
    */
  private def getColumnsList(from: String, sourceColumnList: Array[String] = Array()): Seq[Column] = {
    val columnList =
      new Column(s"$TargetAlias.$surrogateKey") +:
        (keyColumns ++ nonKeyColumns).map(column => new Column(s"$from.${column.toString}"))

    // For derived columns, if target select as is or select from source
    val derivedColumnList = from match {
      case TargetAlias => derivedColumns.map(columnName => $"$from.$columnName")
      case SourceAlias => derivedColumns.map(columnName =>
        if (sourceColumnList.contains(columnName))
          $"$SourceAlias.$columnName"
        else
          $"$TargetAlias.$columnName")
    }

    val metaColumnList = from match {
      case TargetAlias => metaColumns.map(columnName => $"$from.$columnName")
      case SourceAlias => metaColumns.map(columnName => lit(null).as(columnName))
    }

    columnList ++ derivedColumnList ++ metaColumnList :+ $"$SCDStatus"
  }

  /**
    * Generate MD5 values for key & non-key Columns in Source and Dimension table
    *
    * @param sourceData [[DataFrame]] for source
    * @return [[Tuple2]]
    */
  private def generateMD5Columns(sourceData: DataFrame): (DataFrame, DataFrame) = {

    // Validate if source dataframe has provided columns
    if (!validate(sourceData, (keyColumns ++ nonKeyColumns).map(_.toString)))
      throw new SparkException("Provided Source Dataframe does not contain the natural key and/or non-key columns")

    // source md5 calculation
    val sourceWithMD5 = sourceData.withColumn(MD5SourceKeyValueColumn, md5(lower(concat_ws(Separator, keyColumns: _*))))
      .withColumn(MD5SourceNonKeyValueColumn, md5(lower(concat_ws(Separator, nonKeyColumns: _*))))

    // dimension data md5 calculation
    val currentTargetWithMD5 = dimensionDF.withColumn(MD5TargetKeyValueColumn, md5(lower(concat_ws(Separator, keyColumns: _*))))
      .withColumn(MD5TargetNonKeyValueColumn, md5(lower(concat_ws(Separator, nonKeyColumns: _*))))

    (sourceWithMD5.alias(SourceAlias), currentTargetWithMD5.alias(TargetAlias))
  }

  /**
    * Get Dimension change set based on the MD5 values for key and non-key columns
    *
    * @param sourceData      [[DataFrame]] of source data
    * @param incrementalLoad Flag denoting whether it is a incremental load or full load
    * @return [[DimensionChangeSet]]
    */
  def getDimensionChangeSet(sourceData: DataFrame, incrementalLoad: Boolean = true): DimensionChangeSet = {

    val (sourceWithMD5, currentTargetWithMD5) = generateMD5Columns(sourceData)

    val joinedMD5 = sourceWithMD5.join(currentTargetWithMD5,
      $"$MD5SourceKeyValueColumn" === $"$MD5TargetKeyValueColumn", FullJoinType)
      .persist()

    val dimensionChangesDF = joinedMD5.withColumn(SCDStatus, udf(identifyChanges _).apply($"$MD5SourceKeyValueColumn",
      $"$MD5SourceNonKeyValueColumn", $"$MD5TargetKeyValueColumn", $"$MD5TargetNonKeyValueColumn", lit(incrementalLoad)))

    // Insert records
    val insertRecords = dimensionChangesDF.filter($"$SCDStatus" === Insert)
      .select(getColumnsList(SourceAlias, sourceData.columns): _*)

    // Update records
    val updateRecords = dimensionChangesDF.filter($"$SCDStatus" === Update)
      .select(getColumnsList(SourceAlias, sourceData.columns): _*)

    // NCD records
    val noChangeDeleteRecords = dimensionChangesDF.filter($"$SCDStatus" === NoChangeDelete)
      .select(getColumnsList(TargetAlias): _*)

    // NCD records
    val invalidRecords = dimensionChangesDF.filter($"$SCDStatus" === Invalid)
      .select(getColumnsList(TargetAlias): _*)

    DimensionChangeSet(insertRecords, updateRecords, noChangeDeleteRecords, invalidRecords)
  }


}

object DimensionTable {

  // Constants
  val MD5SourceKeyValueColumn = "md5_key_value_source"
  val MD5TargetKeyValueColumn = "md5_key_value_target"
  val MD5SourceNonKeyValueColumn = "md5_nonkey_value_source"
  val MD5TargetNonKeyValueColumn = "md5_nonkey_value_target"
  val SCDStatus = "scd_status"
  val SourceAlias = "source"
  val TargetAlias = "target"
  val FullJoinType = "full"
  val Separator = ":"

  // SCD Update statuses
  val Insert = "I"
  val Update = "U"
  val NoChangeDelete = "NCD"
  val Invalid = "INVALID"

  /**
    * Class represents Dimension Change Set
    *
    * @param insertRecords           Insert Records for Dimension
    * @param updateRecords           Update Records for Dimension
    * @param noChangeOrDeleteRecords No Change or Delete Records for Dimension
    * @param invalidRecords          Invalid Records for Dimension, This [[DataFrame]] is populated only during Full load mode,
    *                                wherein it will contain records with natural keys not contained in source.
    */
  case class DimensionChangeSet(insertRecords: DataFrame,
                                updateRecords: DataFrame,
                                noChangeOrDeleteRecords: DataFrame,
                                invalidRecords: DataFrame)

  /**
    * Identify SCD status
    *
    * @param sourceKeyMd5    Source MD5
    * @param sourceNonKeyMd5 SourceNonKeyMD5
    * @param targetKeyMd5    TargetKeyMD5
    * @param targetNonKeyMd5 TargetKeyMD5
    * @param incremental     Incremental Load
    * @return SCD Status
    */
  def identifyChanges(sourceKeyMd5: String, sourceNonKeyMd5: String,
                      targetKeyMd5: String, targetNonKeyMd5: String,
                      incremental: Boolean): String = {
    if (targetKeyMd5 == null)
      Insert
    else if (sourceKeyMd5 == targetKeyMd5 && sourceNonKeyMd5 != targetNonKeyMd5)
      Update
    else if (incremental && ((sourceKeyMd5 == targetKeyMd5 && sourceNonKeyMd5 == targetNonKeyMd5) || sourceKeyMd5 == null))
      NoChangeDelete
    else if (!incremental && (sourceKeyMd5 == targetKeyMd5 && sourceNonKeyMd5 == targetNonKeyMd5))
      NoChangeDelete
    else
      Invalid
  }
}