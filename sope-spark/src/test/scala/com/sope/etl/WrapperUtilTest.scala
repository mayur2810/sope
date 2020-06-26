package com.sope.etl

import com.sope.spark.utils.WrapperUtil
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author mbadgujar
  */
class WrapperUtilTest extends FlatSpec with Matchers {

  "WrapperUtil" should "parse and generate the Spark options correctly" in {
    val args = Array("--yaml_folders=.", "--main_yaml_file=demo.yaml", "--cluster_mode=false",
      "--name=test_run", "--num-executors=5")
    WrapperUtil.main(args)
  }

  "WrapperUtil" should "throw exception for incorrect options" in {
    val args = Array("--yaml_folders=/yaml_folder/path", "--main_yaml_file=demo.yaml", "--cluster_mode=flse")
    intercept[Exception] {
      WrapperUtil.main(args)
    }
  }

}
