package com.sope.etl.transform.exception

/**
  * Yaml Data Transformer Custom Exception
  *
  * @author mbadgujar
  */
class YamlDataTransformException(msg: String) extends Exception {
  override def getMessage: String = msg
}
