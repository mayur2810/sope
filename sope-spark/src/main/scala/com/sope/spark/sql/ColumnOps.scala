package com.sope.spark.sql

import com.sope.common.sql.{SqlColumnOps, Types}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.callUDF

/**
 * @author mbadgujar
 */
object ColumnOps extends SqlColumnOps[Column] {

  override def resolveSingleArgFunction(functionName: String): Types.ColFunc[Column] = callUDF(functionName, _)

  override def resolveMultiArgFunction(functionName: String): Types.MultiColFunc[Column] =
    (columns: Seq[Column]) => callUDF(functionName, columns: _*)

}
