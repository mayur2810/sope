package com.sope.common.yaml

/**
  * Yaml file representing List data
  *
  * @param yamlPath the Yaml File
  * @tparam V Type
  * @author mbadgujar
  */
class ListYaml[V](yamlPath: String) extends YamlFile(yamlPath, None, classOf[Seq[V]]) {
  def getList: Seq[V] = model
}
