package com.sope.common.transform.model.io.input

import com.fasterxml.jackson.annotation.{JsonProperty, JsonTypeInfo}

/**
 * @author mbadgujar
 */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type")
abstract class SourceTypeRoot[D](@JsonProperty(value = "type", required = true) id: String, alias: String) {

  def apply: _ => D

  def getSourceName: String = alias

  def getId: String = id
}