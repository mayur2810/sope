package com.sope.etl.yaml

import com.sope.etl.TestContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * @author mbadgujar
  */
class ParallelizeYamlTest extends AnyFlatSpec with Matchers {

  "ParallelizeYaml" should "should parallelize the local yaml to Dataframe correctly" in {
    val context = TestContext.getSQlContext
    val df = ParallelizeYaml("data/local_data.yaml").parallelize(context)
    df.count() should be(2)
    df.show()
  }
}
