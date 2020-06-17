package com.sope.etl.transform

import com.sope.etl.SopeETLConfig
import com.sope.etl.transform.exception.YamlDataTransformException
import com.sope.etl.transform.model._
import com.sope.etl.transform.model.action.JoinAction
import com.sope.spark.sql.dsl._
import com.sope.utils.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success, Try}

/**
  * Contains logic for building Spark SQL transformations
  *
  * @author mbadgujar
  */
class Transformer(file: String, inputMap: Map[String, DataFrame], model: TransformModel) extends Logging {


  private case class InputSource(name: String, isUsedForJoin: Boolean, joinColumns: Option[Seq[String]])

  private val autoPersistSetting = SopeETLConfig.AutoPersistConfig
  private var sourceDFMap: Map[String, DataFrame] = inputMap
  private val transformations = model.transformations.data

  // Generate the input sources for the transformation
  private lazy val inputSources = transformations.flatMap { transform =>
    transform.actionList.foldLeft(Nil: Seq[InputSource]) {
      case (inputs, action) =>
        val (isJoinAction, joinColumns) =
          action match {
            case ja: JoinAction if !ja.expressionBased => (true, Some(ja.joinColumns))
            case _ => (false, None)
          }
        inputs ++ action.inputAliases.map(alias => InputSource(alias, isJoinAction, joinColumns))
    } :+ InputSource(transform.source, isUsedForJoin = false, None)
  } ++ model.targets.data.map(target => InputSource(target.getInput, isUsedForJoin = false, None)) // add inputs from target information

  /**
    * Check if the alias that is to be persisted can be
    * pre partitioned if used in multiple joins using same columns
    *
    * @param alias Transformation alias
    * @return Partitioning columns
    */
  private def prePartitionColumns(alias: String): Option[Seq[String]] = {
    val joinSources = inputSources
      .filter(source => source.name == alias && source.isUsedForJoin)
      .map(source => source.joinColumns.getOrElse(Nil).sorted)
    if (joinSources.nonEmpty && joinSources.size >= 2) Some(joinSources.maxBy(_.mkString(","))) else None
  }

  // Initialize the auto persist mapping for sources
  val autoPersistList: Seq[String] = inputSources
    .filter(_ => autoPersistSetting)
    .map(_.name -> 1)
    .groupBy(_._1)
    .map { case (k, v) => (k, v.size) }
    .filter(_._2 > 1).keys
    .toSeq

  // gets dataframe from provided alias
  private def getDF(alias: String): DataFrame = {
    if (!sourceDFMap.isDefinedAt(alias))
      throw new YamlDataTransformException(s"Alias: $alias not found")

    val autoPersist = autoPersistList.contains(alias)
    val df = sourceDFMap(alias).storageLevel match {
      case level: StorageLevel if level == StorageLevel.NONE && `autoPersist` && !sourceDFMap(alias).isStreaming  =>
        logWarning(s"Auto persisting transformation: '$alias' in Memory only mode")
        val persisted = (prePartitionColumns(alias) match {
          case Some(sortCols) =>
            logWarning(s"Persisted transformation: '$alias' will be pre-partitioned on columns: ${sortCols.mkString(", ")}")
            sourceDFMap(alias).repartition(sortCols.map(col): _*)
          case None =>
            sourceDFMap(alias)
        }).persist(StorageLevel.MEMORY_ONLY)
        sourceDFMap = sourceDFMap updated(alias, persisted)
        persisted
      case _ => sourceDFMap(alias)
    }
    logDebug(s"Schema for transformation $alias :-\n${df.schema.treeString}")
    df
  }

  /**
    * Applies the provided list of transformations on the sources (dataframes)
    *
    * @return Transformed Dataframes
    */
  def transform: Seq[(String, DataFrame)] = {

    logDebug("AUTO persist set: " + autoPersistSetting)
    logDebug("AUTO persist data list: " + autoPersistList.mkString(", "))

    inputMap.toSeq ++ transformations.flatMap(dfTransform => {
      val transformAliases = dfTransform.getAliases
      logInfo(s"Applying transformation: ${transformAliases.mkString(",")}")
      val actions = dfTransform.actionList
      val sourceDF = getDF(dfTransform.source)
      // if sql transform apply sql or perform provided action transformation
      val transformedDF = Try {
        (dfTransform.isSQLTransform, dfTransform.isMultiOutputTransform) match {
          case (true, _) =>
            Nil :+ sourceDF.sqlContext.sql(dfTransform.sql.get)
          case (_, true) =>
            // TODO currently considers multi action as last, can be anywhere?
            val multiOutAction = actions.last
            val transformedSingleAction = actions
              .take(actions.size - 1)
              .foldLeft(NoOp()) {
                (transformed, transformAction) => transformed + transformAction.runtimeModifier(transformAction.inputAliases.map(getDF): _*).head
              }
            multiOutAction
              .apply(multiOutAction.inputAliases.map(getDF): _*)
              .map(action => transformedSingleAction + action --> sourceDF)
          case (_, _) =>
            Nil :+ actions.foldLeft(NoOp()) {
              (transformed, transformAction) =>
                transformed + transformAction.runtimeModifier(transformAction.inputAliases.map(getDF): _*).head
            } --> sourceDF
        }
      } match {
        case Success(df) => df
        case Failure(e) =>
          logError(s"Transformation failed for alias(es): ${transformAliases.mkString(", ")} in $file file")
          throw e
      }
      // Add alias to dataframe
      dfTransform.persistLevel.fold(transformedDF)(level => {
        logInfo(s"Transformation ${transformAliases.mkString(",")} is configured to be persisted at level: $level")
        transformedDF.map(_.persist(StorageLevel.fromString(level.toUpperCase)))
      })
        .zip(transformAliases)
        .foreach { case (df, alias) =>
          val aliasedDF = df.alias(alias)
          // Update Map
          sourceDFMap = sourceDFMap updated(alias, aliasedDF)
          // Create temp view
          aliasedDF.createOrReplaceTempView(alias)
        }
      transformAliases.map(alias => (alias, getDF(alias)))
    })
  }

}
