package com.sope.yaml

import com.sope.spark.yaml.SchemaYaml
import org.scalatest.{FlatSpec, Matchers}
/**
  *
  * @author mbadgujar
  */
class SchemaYamlTest extends FlatSpec with Matchers {

  "Schema Yaml Parse " should "should generate the Spark schema corretcly" in {
    val yamlFile = SchemaYaml("schema.yaml")
    val sparkSchema = yamlFile.getSparkSchema
    sparkSchema.fields.length should be(7)
  }

}
