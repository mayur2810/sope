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


  /**
   * Applies know arguments to MultiArg Function in order provided and returns
   * a single arg function to which the actual column can be provided
   *
   * @param multiArgFunc Multi Argument Column Function
   * @param columns      Columns to be applied
   * @return [[ColFunc]]
   */
  def multiArgToSingleArgFunc(multiArgFunc: Types.MultiColFunc[Column], columns: Seq[Column]): ColFunc =
    (column: Column) => multiArgFunc(column +: columns)

}
