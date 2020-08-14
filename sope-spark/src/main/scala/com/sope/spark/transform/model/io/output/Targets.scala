package com.sope.spark.transform.model.io.output

import com.fasterxml.jackson.databind.jsontype.NamedType
import com.sope.common.transform.model.TransformationTypeRegistration

/**
 * @author mbadgujar
 */
class Targets extends TransformationTypeRegistration {
  override def getTypes: List[NamedType] = List(
    new NamedType(classOf[HiveTarget], "hive"),
    new NamedType(classOf[OrcTarget], "orc"),
    new NamedType(classOf[ParquetTarget], "parquet"),
    new NamedType(classOf[CSVTarget], "csv"),
    new NamedType(classOf[TextTarget], "text"),
    new NamedType(classOf[JsonTarget], "json"),
    new NamedType(classOf[JDBCTarget], "jdbc"),
    new NamedType(classOf[CustomTarget], "custom"),
    new NamedType(classOf[CountOutput], "count"),
    new NamedType(classOf[ShowOutput], "show")
  )
}
