package com.sope

import com.sope.utils.Logging

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

/**
  * Constants & Utility Methods
  *
  * @author mbadgujar
  */
package object etl extends Logging {

  val UDFRegistrationClassProperty = "sope.etl.udf.class"

  val TransformationRegistrationClassProperty = "sope.etl.transformation.class"

  val AutoPersistProperty = "sope.auto.persist"

  /**
    * Get Scala 'Object' instance from class name
    *
    * @param clsName Class Name
    * @tparam A Object Type
    * @return Option of type A
    */
  def getObjectInstance[A](clsName: String): Option[A] = {
    val mirror = runtimeMirror(this.getClass.getClassLoader)
    val module = mirror.staticModule(clsName)
    Some(mirror.reflectModule(module).instance.asInstanceOf[A])
  }

  /**
    * Get Class Instance A from provided class name
    *
    * @param clsName Class Name
    * @tparam A Class type
    * @return Option of type A
    */
  def getClassInstance[A](clsName: String): Option[A] = Try {
    val clazz = this.getClass.getClassLoader.loadClass(clsName)
    clazz.newInstance().asInstanceOf[A]
  } match {
    case Success(c) => Some(c)
    case Failure(e) => e match {
      case _: java.lang.InstantiationException => getObjectInstance[A](clsName)
      case _ =>
        logError(s"Failed to load class  : $clsName")
        None
    }
  }
}
