package com.sope.common.transform

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.sope.common.annotations.SqlExpr
import com.sope.common.transform.exception.TransformException
import com.sope.common.transform.model.MListDeserializer._
import com.sope.common.transform.model.action.TransformActionRoot
import com.sope.common.transform.model.io.input.SourceTypeRoot
import com.sope.common.transform.model.io.output.TargetTypeRoot

/**
 * Package contains YAML Transformer Root construct mappings and definitions
 *
 * @author mbadgujar
 */
package object model {

  /**
   * Structure representing a 'Model List' that is parsed to corresponding Model type.
   * Any Failures during deserialization are collected for reporting purpose.
   * Refer [[MListDeserializer]] for more details.
   *
   * @param data     Deserialized data
   * @param failures Failed records of the List
   * @tparam T Type
   */
  case class MList[T](data: Seq[T], failures: Seq[Failed] = Nil, sqlExprs: Seq[SqlExpression] = Nil)

  /**
   * Structure for maintaining failure information
   *
   * @param message Failure message
   * @param line    Line number
   * @param index   Column position
   */
  case class Failed(message: String, line: Int = -1, index: Int = -1)

  case class SqlExpression(expr: Any, isSql: Boolean, line: Int = -1, index: Int = -1)

  /**
   * Base Trait for Transformation Model
   */
  trait TransformModel[D] {
    /**
     * Get the sources involved
     *
     * @return Seq[_]
     */
    def sources: MList[_]

    /**
     * Transformations list
     *
     * @return Seq[DFTransformation]
     */
    def transformations: MList[_ <: Transformation[D]]

    /**
     * Output Targets
     *
     * @return Seq[TargetTypeRoot]
     */
    def targets: MList[_ <: TargetTypeRoot[D]]


    /**
     * Validation function for SQL expression
     *
     * @param sqlExpr The SQL object
     * @param isSQL   If the pass SQL if complete SQL string
     * @return Error message if the validation failed
     */
    def sqlValidationFn(sqlExpr: Any, isSQL: Boolean): Option[String]

    /**
     * Validates the SQL elements of the Model
     *
     * @return List of Failed elements
     */
    def checkSQLExpressions: Seq[Failed] = {
      val exprs = sources.sqlExprs ++
        targets.sqlExprs ++
        transformations.sqlExprs ++
        transformations.data.flatMap(_.actions.sqlExprs)
      exprs.flatMap { expr =>
        val errorMsg = sqlValidationFn(expr.expr, expr.isSql)
        if (errorMsg.isDefined)
          Nil :+ Failed(errorMsg.get, expr.line, expr.index)
        else
          None
      }
    }

    /**
     * Check if the model has any failed elements during deserialization
     *
     * @return Tuple of failed status and list of failures in case their is failure
     */
    def hasFailures: (Boolean, Seq[Failed]) = {
      val failures = sources.failures ++
        targets.failures ++
        transformations.failures ++
        transformations.data.flatMap(_.actions.failures)
      (failures.nonEmpty, failures ++ checkSQLExpressions)
    }

    /**
     * Checks for failure and throws [[TransformException]] if there are any.
     */
    def checkFailure(): Unit = {
      val (hasFailure, failedList) = hasFailures
      if (hasFailure)
        throw TransformException(s"Parsing Failed", failedList)
    }

  }

  /**
   * Class represents a transformation entity.
   *
   * @param source      input source name
   * @param alias       alias for the transformation
   * @param options     Transformation options
   * @param description Description for this transformation
   * @param actions     Actions to performed on source. Either 'actions' or 'sql' should be provided
   * @param sql         Transformation provided as sql query. Either 'sql' or 'actions' should be provided
   */
  case class Transformation[D](@JsonProperty(required = true, value = "input") source: String,
                               alias: Option[String],
                               aliases: Option[Seq[String]],
                               options: Option[Map[String, Any]],
                               description: Option[String],
                               @JsonDeserialize(using = classOf[ActionDeserializer[D]])
                               actions: MList[TransformActionRoot[D]],
                               @SqlExpr sql: Option[String]) {

    val actionList: Seq[TransformActionRoot[D]] = actions.data

    // validate transformation options
    if (actionList.nonEmpty && sql.isDefined)
      throw new TransformException("Please provide either 'actions' or 'sql' option in transformation construct, not " +
        "both..")

    if (actionList.isEmpty && sql.isEmpty && actions.failures.isEmpty)
      throw new TransformException("Please provide either 'actions' or 'sql' option in transformation construct")

    // validate aliasing options
    if (alias.isDefined && aliases.isDefined)
      throw new TransformException("Please provide either 'alias' or 'aliases' option for naming transformations, not" +
        " both")

    if (alias.isEmpty && aliases.isEmpty)
      throw new TransformException("Please provide either 'alias' or 'aliases' option")

    if (aliases.isDefined && (aliases.get.isEmpty || aliases.get.exists(_.isEmpty)))
      throw new TransformException("aliases option cannot be empty")

    if (alias.isDefined && alias.get.isEmpty)
      throw new TransformException("alias option cannot be empty")

    val isSQLTransform: Boolean = sql.isDefined

    val isMultiOutputTransform: Boolean = aliases.isDefined

    /**
     * Get Transformation alias.
     *
     * @return alias
     */
    def getAliases: Seq[String] = if (isMultiOutputTransform) aliases.getOrElse(Nil) else alias.getOrElse(source) +: Nil
  }


  // Model for YAML without target information. Sources are virtual aliases
  case class TransformModelWithoutTarget[D](@JsonProperty(required = true, value = "inputs") vSources: Seq[String],
                                            @JsonDeserialize(using = classOf[TransformationDeserializer[D]])
                                            @JsonProperty(required = true) transformations: MList[Transformation[D]])
    extends TransformModel[D] {

    override def sources: MList[String] = MList(vSources)

    override def targets: MList[TargetTypeRoot[D]] = MList(Nil)

    checkFailure()

    override def sqlValidationFn(sqlExpr: Any, isSQL: Boolean): Option[String] = None
  }

  // Model for YAML with source target information
  case class TransformModelWithSourceTarget[CTX, D](@JsonDeserialize(using = classOf[InputDeserializer[CTX, D]])
                                                    @JsonProperty(required = true, value = "inputs")
                                                    sources: MList[SourceTypeRoot[CTX, D]],
                                                    @JsonDeserialize(using = classOf[TransformationDeserializer[D]])
                                                    @JsonProperty(required = true)
                                                    transformations: MList[Transformation[D]],
                                                    @JsonDeserialize(using = classOf[TargetDeserializer[D]])
                                                    @JsonProperty(required = true, value = "outputs")
                                                    targets: MList[TargetTypeRoot[D]],
                                                    configs: Option[Map[String, String]],
                                                    udfs: Option[Map[String, String]],
                                                    @JsonProperty(value = "udf_files") udfFiles: Option[Seq[String]])
    extends TransformModel[D] {
    override def sqlValidationFn(sqlExpr: Any, isSQL: Boolean): Option[String] = None

    checkFailure()
  }

}