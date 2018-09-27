package com.sope.etl.transform

import com.sope.etl.transform.exception.YamlDataTransformException
import com.sope.etl.transform.model.{DFTransformation, TransformModelWithSourceTarget, TransformModelWithoutSourceTarget}
import com.sope.spark.sql.dsl._
import com.sope.utils.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

/**
  * Reads YAML and performs Spark transformations provided in the YAML file
  *
  * @author mbadgujar
  */
class YamlDataTransform(yaml: String, dataFrames: DataFrame*) extends Logging {


  /**
    * Applies the provided list of transformations on the sources (dataframes)
    *
    * @param mapping         Mapping of sources and dataframe objects
    * @param transformations List of [[DFTransformation]]s
    * @return Transformed Dataframes
    */
  private def applyTransformations(mapping: Map[String, DataFrame],
                                   transformations: Seq[DFTransformation]): Seq[(String, DataFrame)] = {
    var sourceDFMap = mapping
    val autoPersistSetting = Option(System.getProperty("dwp.auto.persist")).fold(true)(_.toBoolean)

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

    logDebug("AUTO persist set: " + autoPersistSetting)
    logDebug("AUTO persist data list: " + autoPersistList.mkString(", "))

    // gets dataframe from provided alias
    def getDF(alias: String): DataFrame = {
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
        dfTransform.actions.get.foldLeft(NoOp()) {
          case (transformed, transformAction) => transformed + transformAction(transformAction.inputAliases.map(getDF): _*)
        } --> sourceDF
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


  /**
    * Performs end to end transformations - Reading sources and writing transformation result to provided targets
    * The source yaml file should contains source and target information.
    *
    * @param sqlContext Spark [[SQLContext]]
    */
  def performTransformations(sqlContext: SQLContext): Unit = {
    val transformModel = YamlParserUtil.parseYAML(yaml, classOf[TransformModelWithSourceTarget])
    val sourceDFMap = transformModel.sources.map(source => source.getSourceName
      -> source.apply(sqlContext).alias(source.getSourceName)).toMap
    val transformationResult = applyTransformations(sourceDFMap, transformModel.transformations).toMap
    transformModel.targets.foreach(target => target(transformationResult(target.getInput)))
  }


  /**
    * Perform transformation on provided dataframes.
    * The sources provided in YAML file should be equal and in-order to the provided dataframes
    *
    * @return Transformed [[DataFrame]]
    */
  def getTransformedDFs: Seq[(String, DataFrame)] = {
    val transformModel = YamlParserUtil.parseYAML(yaml, classOf[TransformModelWithoutSourceTarget])
    if (transformModel.sources.size != dataFrames.size)
      throw new YamlDataTransformException("Invalid Dataframes provided or incorrect yaml config")
    val sourceDFMap = transformModel.sources.zip(dataFrames).map { case (source, df) => (source, df.alias(source)) }
    applyTransformations(sourceDFMap.toMap, transformModel.transformations)
  }
}
