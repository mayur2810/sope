package com.sope.common.yaml

import scala.collection.mutable

/**
  * Yaml file representing a Map type
  *
  * @param yamlPath Yaml File path
  * @tparam K Key class
  * @tparam V Value class
  * @author mbadgujar
  */
class MapYaml[K, V](yamlPath: String) extends YamlFile(yamlPath, None, classOf[mutable.LinkedHashMap[K, V]]) {
  def getMap: Map[K, V] = model.toMap
}
