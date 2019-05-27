package com.sope.etl.yaml

import com.sope.etl.TestContext
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author mbadgujar
  */
class ParallelizeYamlTest extends FlatSpec with Matchers {

  "ParallelizeYaml" should "should parallelize the local yaml to Dataframe correctly" in {
    val context = TestContext.getSQlContext
    val df = ParallelizeYaml("local_data.yaml").parallelize(context)
    df.count() should be(2)
    df.show()
  }
}
