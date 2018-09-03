package com.mayurb.dwp.transform

import java.io.FileReader

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.mayurb.dwp.transform.exception.YamlDataTransformException
import com.mayurb.dwp.transform.model.{DFTransformation, TransformModel, TransformModelWithSourceTarget, TransformModelWithoutSourceTarget}
import com.mayurb.spark.sql.dsl._
import com.mayurb.utils.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable

/**
  * Reads YAML and performs Spark transformations provided in the YAML file
  *
  * @author mbadgujar
  */
class YamlDataTransform(yamlFilePath: String, dataFrames: DataFrame*) extends Logging {


  /**
    * Parses the YAML file to [[TransformModel]] object
    *
    * @return [[TransformModel]]
    */
  def parseYAML(containsSourceInfo: Boolean): TransformModel = {
    // Instantiate object mapper object
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    if (containsSourceInfo)
      mapper.readValue(new FileReader(yamlFilePath), classOf[TransformModelWithSourceTarget])
    else {
      val model = mapper.readValue(new FileReader(yamlFilePath), classOf[TransformModelWithoutSourceTarget])
      if (model.sources.size != dataFrames.size)
        throw new YamlDataTransformException("Invalid Dataframes provided or incorrect yaml config")
      model
    }
  }


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
    val autoPersistMapping: mutable.Map[String, Boolean] = {
      lazy val persistSeq = transformations
        .flatMap(transform => transform.source +: transform.actions.fold(Nil: Seq[String])(actions =>
          actions.foldLeft(Nil: Seq[String]) { case (a1, a2) => a1 ++ a2.inputAliases }))
        .map(_ -> 1)
        .groupBy(_._1).map { case (k, v) => (k, v.size) }
        .filter(_._2 > 1)
        .map(_._1 -> false).toSeq
      if (autoPersistSetting) mutable.Map[String, Boolean](persistSeq: _*) else mutable.Map[String, Boolean]()
    }

    logDebug("Auto persist set: " + autoPersistSetting)
    logDebug("Auto persist data list: " + autoPersistMapping.keys.mkString(", "))

    // gets dataframe from provided alias
    def getDF(alias: String): DataFrame = {
      if (sourceDFMap.isDefinedAt(alias)) {
        if (autoPersistMapping.isDefinedAt(alias) && !autoPersistMapping.getOrElse(alias, false)) {
          logWarning(s"AUTO persisting $alias, since it is used multiple times")
          autoPersistMapping.update(alias, true)
          sourceDFMap(alias).persist()
        }
        else sourceDFMap(alias)
      }
      else
        throw new YamlDataTransformException(s"Alias: $alias not found")
    }


    transformations.map(dfTransform => {
      logInfo(s"Applying transformation for source: ${dfTransform.source}")
      val persistTransformation = dfTransform.persist
      logInfo(s"Transformation will be persisted: $persistTransformation")
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
        if (persistTransformation) transformedDF.persist else transformedDF
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
    val transformModel = parseYAML(true).asInstanceOf[TransformModelWithSourceTarget]
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
    val transformModel = parseYAML(false).asInstanceOf[TransformModelWithoutSourceTarget]
    val sourceDFMap = transformModel.sources.zip(dataFrames).map { case (source, df) => (source, df.alias(source)) }
    applyTransformations(sourceDFMap.toMap, transformModel.transformations)
  }
}
