package com.sope.spark.sql

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

import scala.util.Try

/**
  *
  * @author mbadgujar
  */
package object udfs {

  // Ordering for sql.Date
  implicit val dateOrdering: Ordering[Date] = new Ordering[Date] {
    def compare(x: Date, y: Date): Int = x compareTo y
  }.reverse


  /*===================
      Common Functions
    ===================*/

  /**
    * Check if the column value is not empty
    *
    * @param value column value
    * @return [[Boolean]]
    */
  def isNotEmpty(value: Any): Boolean = {
    Option(value) match {
      case None => false
      case Some(str: String) => str.trim.nonEmpty
      case Some(_) => true
    }
  }

  /**
    * Converts String to Long
    *
    * @param str Input string
    * @return Long
    */
  def strToLong(str: String): Long = Option(str) match {
    case None => 0L
    case Some("") => 0L
    case Some(id) => id.toLong
  }

  /**
    * Check Date format
    *
    * @param dateStr Date String
    * @param format  Date format
    * @return [[Boolean]]
    */
  def checkDateFormat(dateStr: String, format: String): Boolean = {
    val simpleDateFormat = new SimpleDateFormat(format)
    Try(simpleDateFormat.parse(dateStr)).isFailure
  }

  /**
    * Merge Function for merging records with information across different dates
    * If value is present for latest date, it is considered or not null value for next date is considered
    *
    * @param orderingColumn Date Column for orderingmm
    * @param rows           Rows to be merged
    * @return Merged Row
    */
  def mergeFunc(orderingColumn: String, rows: Seq[Row]): Row = {
    val sorted = rows.map(row => (row.getAs[Date](orderingColumn), row)).sortBy(_._1)
    val merged = sorted.map(_._2.toSeq).foldLeft(Seq[Any]()) { case (row1, row2) =>
      row1 match {
        case Nil => row2
        case _ => (row1 zip row2).map { case (value1, value2) => if (value1 == null || value1.toString.isEmpty) value2 else value1 }
      }
    }
    Row.fromSeq(merged)
  }


  /*===================
          UDAFs
  ===================*/

  /**
    * UDAF to group struct type columns to List/Array type
    *
    * @param struct [[StructType]] for struct column to be grouped
    */
  class CollectStruct(struct: StructType) extends UserDefinedAggregateFunction {

    override def inputSchema: org.apache.spark.sql.types.StructType = StructType(Seq(StructField("toBeGroupedCols", struct)))

    override def bufferSchema: StructType = StructType(Seq(StructField("outputList", ArrayType(struct))))

    override def dataType: DataType = ArrayType(struct)

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = Nil
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getSeq[Any](0) :+ input.getStruct(0)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getSeq[Any](0) ++ buffer2.getSeq[Any](0)
    }

    override def evaluate(buffer: Row): Any = {
      buffer.get(0)
    }
  }


  /*===================
          UDFs
  ===================*/

  // Check if string column is empty
  def checkEmptyUDF: UserDefinedFunction = udf(!isNotEmpty(_: Any))

  // Check if string column is non-empty
  def checkNotEmptyUDF: UserDefinedFunction = udf(isNotEmpty(_: Any))

  //converts string to long
  def strToLongUDF: UserDefinedFunction = udf(strToLong(_: String))

  def dateFormatCheckUdf: UserDefinedFunction = udf(checkDateFormat(_: String, _: String))

  def mergeUDF[T](orderingDateColumn: String, orderingColumnType: Class[T], outputStructure: StructType): UserDefinedFunction = {
     //udf(mergeFunc(orderingDateColumn, _: Seq[Row]), groupedSchema, col(groupedColumn))
    null
   }


  def registerUDFs(sqlContext: SQLContext): Unit = {
    Map(
      "check_empty" -> checkEmptyUDF,
      "check_not_empty" -> checkEmptyUDF,
      "str_to_long" -> strToLongUDF,
      "date_format_check" -> dateFormatCheckUdf)
      .foreach { case (name, udf) => sqlContext.udf.register(name, udf) }
  }

}
