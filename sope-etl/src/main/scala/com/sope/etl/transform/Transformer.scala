package com.sope.etl.transform

import com.sope.etl.transform.exception.YamlDataTransformException
import com.sope.etl.transform.model._
import com.sope.spark.sql.dsl._
import com.sope.utils.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success, Try}

/**
  * Contains logic for building Spark SQL transformations
  *
  * @author mbadgujar
  */
class Transformer(file: String, inputMap: Map[String, DataFrame], transformations: Seq[DFTransformation]) extends Logging {


  private val autoPersistSetting = Option(System.getProperty("sope.auto.persist")).fold(true)(_.toBoolean)

  // Initialize the auto persist mapping for sources
  val autoPersistList: Seq[String] = {
    lazy val persistSeq = transformations
      .flatMap(transform => transform.source +: transform.actions.fold(Nil: Seq[String])(actions =>
        actions.foldLeft(Nil: Seq[String]) { case (a1, a2) => a1 ++ a2.inputAliases }))
      .map(_ -> 1)
      .groupBy(_._1).map { case (k, v) => (k, v.size) }
      .filter(_._2 > 1).keys
    if (autoPersistSetting) persistSeq.toSeq.distinct else Nil
  }


  // gets dataframe from provided alias
  private def getDF(alias: String) (implicit sourceDFMap: Map[String, DataFrame]): DataFrame = {
    if (sourceDFMap.isDefinedAt(alias)) {
      val autoPersist = autoPersistList.contains(alias)
      sourceDFMap(alias).storageLevel match {
        case level: StorageLevel if level == StorageLevel.NONE && `autoPersist` =>
          logWarning(s"AUTO persisting (Memory only) transformation: $alias, since it is used multiple times")
          sourceDFMap(alias).persist(StorageLevel.MEMORY_ONLY)
        case _ => sourceDFMap(alias)
      }
    }
    else
      throw new YamlDataTransformException(s"Alias: $alias not found")
  }

  /**
    * Applies the provided list of transformations on the sources (dataframes)
    *
    * @return Transformed Dataframes
    */
  def transform : Seq[(String, DataFrame)] = {
    implicit var sourceDFMap: Map[String, DataFrame] = inputMap
    logDebug("AUTO persist set: " + autoPersistSetting)
    logDebug("AUTO persist data list: " + autoPersistList.mkString(", "))

    transformations.map(dfTransform => {
      logInfo(s"Applying transformation: ${dfTransform.getAlias}")
      val sourceDF = getDF(dfTransform.source)
      // coalesce function
      val coalesceFunc = (df: DataFrame) => if (dfTransform.coalesce == 0) df else df.coalesce(dfTransform.coalesce)
      // if sql transform apply sql or perform provided action transformation
      val transformedDF = if (dfTransform.isSQLTransform) {
        sourceDF.createOrReplaceTempView(dfTransform.source)
        sourceDF.sqlContext.sql(dfTransform.sql.get)
      } else {
        Try {
          dfTransform.actions.get.foldLeft(NoOp()) {
            case (transformed, transformAction) => transformed + transformAction(transformAction.inputAliases.map(getDF): _*)
          } --> sourceDF
        } match {
          case Success(df) => df
          case Failure(e) =>
            logError(s"Transformation failed for alias: ${dfTransform.getAlias} in $file file")
            throw e
        }
      }.transform(coalesceFunc)

      // Add alias to dataframe
      val transformedWithAliasDF = {
        dfTransform.persistLevel.fold(transformedDF)(level => {
          logInfo(s"Transformation ${dfTransform.getAlias} is configured to be persisted at level: $level")
          transformedDF.persist(StorageLevel.fromString(level.toUpperCase))
        })
      }.alias(dfTransform.getAlias)
      // Update Map
      sourceDFMap = sourceDFMap updated(dfTransform.getAlias, transformedWithAliasDF)
      (dfTransform.getAlias, transformedWithAliasDF)
    })
  }

}
