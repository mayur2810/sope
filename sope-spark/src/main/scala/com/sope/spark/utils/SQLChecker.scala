package com.sope.spark.utils

import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

import scala.util.control.NonFatal

/**
  * Checks SQL Expression / SQL Statements
  *
  * @author mbadgujar
  */
object SQLChecker {

  // parser with Dummy Conf
  private val parser = new SparkSqlParser(new SQLConf)

  private val checkSQLExpr = (expr: String) => {
    // Skip check if expr has placeholders
    if (".*\\$\\{.*?\\}.*".r.findAllIn(expr).nonEmpty) Unit else parser.parseExpression(expr)
  }

  private def checkSQL(expr: Any, isSQL: Boolean = false): Unit =
    expr match {
      case m: Map[_, _] => m.asInstanceOf[Map[String, Any]].values.foreach(any => checkSQL(any, isSQL))
      case seq: Seq[_] => seq.asInstanceOf[Seq[String]].foreach(checkSQLExpr)
      case str: String => if (isSQL) parser.parsePlan(str) else checkSQLExpr(str)
      case Some(obj) => checkSQL(obj, isSQL)
      case _ => // Do Nothing. Any other cases?
    }

  /**
    * Uses the Spark SQl Parser to check the SQL/SQL expression.
    * Any failures will return the Validation error message, None if validation is successful
    *
    * @param expr  Object to validated
    * @param isSQL True if contains SQL , False if contains sql expression.
    */
  def validateSQLExpression(expr: Any, isSQL: Boolean = false): Option[String] = try {
    checkSQL(expr, isSQL); None
  } catch {
    case NonFatal(e) =>  Some(e.getMessage)
  }

}
