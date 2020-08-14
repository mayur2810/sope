package com.sope.spark.transform.model.io.input

import com.fasterxml.jackson.databind.jsontype.NamedType
import com.sope.common.transform.model.TransformationTypeRegistration

/**
 * @author mbadgujar
 */
class Sources extends TransformationTypeRegistration {
  override def getTypes: List[NamedType] = List(
    new NamedType(classOf[HiveSource], "hive"),
    new NamedType(classOf[OrcSource], "orc"),
    new NamedType(classOf[ParquetSource], "parquet"),
    new NamedType(classOf[CSVSource], "csv"),
    new NamedType(classOf[TextSource], "text"),
    new NamedType(classOf[JsonSource], "json"),
    new NamedType(classOf[JDBCSource], "jdbc"),
    new NamedType(classOf[CustomSource], "custom"),
    new NamedType(classOf[LocalSource], "local")
  )
}
