package com.sope.etl.transform.exception

import com.sope.etl.transform.model.Failed

/**
  * Yaml Data Transformer Custom Exception
  *
  * @author mbadgujar
  */
case class YamlDataTransformException(msg: String, failures: Seq[Failed]) extends Exception {
  def this(msg: String) = this(msg, Nil)

  override def getMessage: String = msg
}
