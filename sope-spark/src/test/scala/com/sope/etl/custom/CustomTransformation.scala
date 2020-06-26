package com.sope.etl.custom

import com.sope.spark.etl.register.TransformationRegistration
import com.sope.spark.sql.MultiDFFunc
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class CustomTransformation extends TransformationRegistration {
  private def transform(df1: DataFrame): DataFrame = {
    df1.withColumn("new_column", lit(true))
  }

  override protected def registerTransformations: Map[String, MultiDFFunc] = Map {
    "custom_transform" -> { dfs: Seq[DataFrame] => transform(dfs.head) }
  }
}
