package com.sope.common.transform.exception

import com.sope.common.transform.model.Failed

/**
  * Yaml Data Transformer Custom Exception
  *
  * @author mbadgujar
  */
case class TransformException(msg: String, failures: Seq[Failed]) extends Exception {
  def this(msg: String) = this(msg, Nil)

  override def getMessage: String = msg
}
