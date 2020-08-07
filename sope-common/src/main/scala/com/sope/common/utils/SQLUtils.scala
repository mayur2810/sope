package com.sope.common.utils

object SQLUtils {

  /**
   * Get SQL Literal Expression for value to substituted in SQL
   *
   * @param value Any value
   * @return String
   */
  def sqlLiteralExpr(value: Any): String = value match {
    case list: List[_] =>
      list
        .map(elem => if (elem.isInstanceOf[String]) s"'${elem.toString}'" else elem)
        .mkString(",")
    case str: String => s"'$str'"
    case _ => value.toString
  }

}
