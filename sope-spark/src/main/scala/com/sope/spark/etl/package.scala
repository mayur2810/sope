package com.sope.spark

import com.sope.common.utils.Logging
import com.sope.spark.etl.register.TransformationRegistration._
import com.sope.spark.etl.register.UDFRegistration._
import com.sope.spark.sql.udfs.registerUDFs
import org.apache.spark.sql.SQLContext

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

/**
  * Constants, Utility methods and objects
  *
  * @author mbadgujar
  */
package object etl extends Logging {

  val MainYamlFileOption = "main_yaml_file"

  val SubstitutionsOption = "substitutions"

  val SubstitutionFilesOption = "substitution_files"

  val UDFRegistrationClassProperty = "sope.etl.udf.class"

  val TransformationRegistrationClassProperty = "sope.etl.transformation.class"

  val AutoPersistProperty = "sope.auto.persist.enabled"

  val TestingModeProperty = "sope.testing.mode.enabled"

  val TestingDataFractionProperty = "sope.testing.data.fraction"


  object TransformationOptions {
    final val PersistLevel = "persist"
  }

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
    * Get Scala 'Object' instance from class name with provided classloader
    *
    * @param classloader Classloader to use
    * @param clsName     Class Name
    * @tparam A Object Type
    * @return Option of type A
    */
  def getObjectInstance[A](classloader: ClassLoader, clsName: String): A = {
    val mirror = runtimeMirror(classloader)
    val module = mirror.staticModule(clsName)
    mirror.reflectModule(module).instance.asInstanceOf[A]
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

  /**
    * Performs Custom UDF & Transformation registrations
    *
    * @param sqlContext Spark's SQL Context
    */
  def performRegistrations(sqlContext: SQLContext): Unit = {
    registerUDFs(sqlContext) // Register sope utility udfs
    registerCustomUDFs(sqlContext) // Register custom udfs if provided
    registerTransformations() // Register custom transformations
  }



  /**
    * Initializes Sope Configurations
    */
  object SopeETLConfig {
    private def getProperty(property: String) = Option(System.getProperty(property))

    val AutoPersistConfig: Boolean = getProperty(AutoPersistProperty).fold(true)(_.toBoolean)
    val TestingModeConfig: Boolean = getProperty(TestingModeProperty).fold(false)(_.toBoolean)
    val TestingDataFraction: Double = getProperty(TestingDataFractionProperty).fold(0.10)(_.toDouble)
    val UDFRegistrationConfig: Option[String] = getProperty(UDFRegistrationClassProperty)
    val TransformationRegistrationConfig: Option[String] = getProperty(TransformationRegistrationClassProperty)
  }

}
