package com.sope.spark.yaml

import java.sql.{Date, Timestamp}

import com.sope.common.yaml.ListYaml
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

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
  def parallelize(sqlContext: SQLContext, schema: Option[StructType] = None): DataFrame = {
    val data = getList
    if (data.isEmpty) return sqlContext.emptyDataFrame
    val externalSchemaMap = schema.getOrElse(Nil).map(field => field.name -> field).toMap
    log.debug(s"Provided external schema :- $externalSchemaMap")
    val sparkSchema = StructType {
      mapToStruct(data.head)
        .map(field => externalSchemaMap.getOrElse(field.name, field))
    }
    log.debug(s"Schema for file $getYamlFileName :- $sparkSchema")
    val rdd = sqlContext.sparkContext
      .parallelize(data)
      .map(mapToRow(_, sparkSchema))
    sqlContext.createDataFrame(rdd, sparkSchema)
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
    * Converts the inferred data object to provided type
    *
    * @param obj      data
    * @param dataType Spark Data Type
    * @return Converted object
    */
  def convertType(obj: Any, dataType: DataType): Any = {
    (dataType, Option(obj).map(_.toString)) match {
      case (ByteType, Some(str)) => str.toByte
      case (IntegerType, Some(str)) => str.toInt
      case (ShortType, Some(str)) => str.toShort
      case (LongType, Some(str)) => str.toLong
      case (DoubleType, Some(str)) => str.toDouble
      case (FloatType, Some(str)) => str.toFloat
      case (DateType, Some(str)) => Date.valueOf(str)
      case (_: DecimalType, Some(str)) => BigDecimal(str)
      case (TimestampType, Some(str)) => Timestamp.valueOf(str)
      case _ => obj
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
  def mapToRow(map: Map[_, _], schema: StructType): Row = Row.fromSeq {
    map.values.zip(schema)
      .map {
        case (innerMap: Map[_, _], field) => mapToRow(innerMap, field.dataType.asInstanceOf[StructType])
        case (obj, field) => convertType(obj, field.dataType)
      }.toSeq
  }
}
