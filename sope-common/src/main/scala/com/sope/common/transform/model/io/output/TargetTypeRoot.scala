package com.sope.common.transform.model.io.output

import com.fasterxml.jackson.annotation.{JsonProperty, JsonTypeInfo}

/**
 * @author mbadgujar
 */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type")
abstract class TargetTypeRoot[D](@JsonProperty(value = "type", required = true) id: String, input: String) {

  def apply(dataset: D): Unit

  def getInput: String = input

  def getId: String = id
}