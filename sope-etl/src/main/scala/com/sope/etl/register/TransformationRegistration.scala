package com.sope.etl.register

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

object TransformationRegistration {

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
}