package com.sope.common.transform.model

import java.util.ServiceLoader

import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.module.SimpleModule

import scala.collection.JavaConverters._

trait TransformationTypeRegistration {
  def getTypes: List[NamedType]
}

object TransformationTypeRegistration {
  def getModule: Option[Module] = {
    val providerClasses = ServiceLoader.load(classOf[TransformationTypeRegistration]).asScala
    val simpleModule = new SimpleModule()
    if (providerClasses.isEmpty) return None
    providerClasses.foreach { provider =>
      simpleModule.registerSubtypes(provider.getTypes: _*)
    }
    Some(simpleModule)
  }
}