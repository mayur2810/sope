package com.sope.etl.yaml

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{StructField, StructType}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Parses a Local Yaml File and parallelises it to a DataFrame.
  * The Schema is inferred from the Yaml data types.
  * Note: A Yaml 'Map' is converted to Struct type in Spark. Hence, a Spark inbuilt Map type is not supported.
  *
  * @author mbadgujar
  */
case class ParallelizeYaml(dataYamlPath: String) extends ListYaml[Map[String, Any]](dataYamlPath) {

  import ParallelizeYaml._

  /**
    * Convert the local Yaml file data to a Dataframe
    *
    * @param sqlContext Spark Context
    * @return Dataframe
    */
  def parallelize(sqlContext: SQLContext): DataFrame = {
    val data = getList
    if (data.isEmpty) return sqlContext.emptyDataFrame
    val schema = mapToStruct(data.head)
    val rdd = sqlContext.sparkContext
      .parallelize(data)
      .map(mapToRow)
    sqlContext.createDataFrame(rdd, StructType(schema))
  }
}

object ParallelizeYaml {

  /**
    * Returns the type for given Object
    *
    * @param t object
    * @return [[Type]]
    */
  private def getType[T: ClassTag](t: T): ScalaReflection.universe.Type = {
    val mirror = runtimeMirror(this.getClass.getClassLoader)
    val base = mirror.reflect(t).symbol
    t match {
      // Gets the internal type of list structure
      case list: List[_] => list.headOption.fold(typeOf[Seq[String]])(elem => {
        val elemType = getType[Any](elem)
        internal.typeRef(NoPrefix, mirror.staticClass("scala.List"), List(elemType))
      })
      case _ => base.selfType
    }
  }

  /**
    * Generates a Spark Struct type schema from given Map
    *
    * @param map Map
    * @return StructFields
    */
  def mapToStruct(map: Map[String, _]): Array[StructField] = {
    map.map {
      case (column, value: Map[_, _]) =>
        val structFields = mapToStruct(value.asInstanceOf[Map[String, _]])
        StructField(column, StructType(structFields))
      case (column, value) =>
        StructField(column, ScalaReflection.schemaFor(getType(value)).dataType)
    }.toArray
  }

  /**
    * Converts a Map object to Spark Row type
    *
    * @param map Map
    * @return Row
    */
  def mapToRow(map: Map[_, _]): Row = Row.fromSeq {
    map.values.map {
      case innerMap: Map[_, _] => mapToRow(innerMap)
      case o => o
    }.toSeq
  }
}
