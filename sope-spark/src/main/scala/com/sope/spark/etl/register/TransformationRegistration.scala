package com.sope.spark.etl.register

import com.sope.common.utils.Logging
import com.sope.spark.etl.{SopeETLConfig, getClassInstance}
import com.sope.spark.sql.MultiDFFunc

import scala.collection.mutable

/**
  * Trait to use for Custom Transformation registration
  *
  * @author mbadgujar
  */
trait TransformationRegistration {

  import TransformationRegistration._

  /**
    * Returns the list of Transformations to Register
    *
    * @return Map of Registration name and transformation
    */
  protected def registerTransformations: Map[String, MultiDFFunc]

  /**
    * Registers the Transformations in Registry
    */
  def performRegistration(): Unit = {
    registerTransformations.foreach {
      case (name, transformation) => registerTransformation(name, transformation)
    }
  }
}

object TransformationRegistration extends Logging {

  private val transformationList = mutable.HashMap[String, MultiDFFunc]()

  /**
    * Register provided transformation name & transformation
    *
    * @param name           Transformation name
    * @param transformation Transformation function
    */
  def registerTransformation(name: String, transformation: MultiDFFunc): Unit =
    transformationList.put(name, transformation)

  /**
    * Get the transformation function by name
    *
    * @param name Transformation name
    * @return Option[MultiDFFunc]
    */
  def getTransformation(name: String): Option[MultiDFFunc] = transformationList.get(name)


  /*
    Registers Custom Transformation from provided class
  */
  def registerTransformations(): Unit = {
    SopeETLConfig.TransformationRegistrationConfig match {
      case Some(classStr) =>
        logInfo(s"Registering custom Transformations from $classStr")
        getClassInstance[TransformationRegistration](classStr) match {
          case Some(transformClass) =>
            transformClass.performRegistration()
            logInfo("Successfully registered Custom Transformations")
          case _ => logError(s"Transformation Registration failed")
        }
      case None => logInfo("No class defined for registering Custom Transformations")
    }
  }
}