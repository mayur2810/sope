package com.sope.etl.yaml
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
/**
  *
  * @author mbadgujar
  */
class SchemaYamlTest extends AnyFlatSpec with Matchers {

  "Schema Yaml Parse " should "should generate the Spark schema corretcly" in {
    val yamlFile = SchemaYaml("schema.yaml")
    val sparkSchema = yamlFile.getSparkSchema
    sparkSchema.fields.length should be(7)
  }

}
