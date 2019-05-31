package com.sope.etl

import com.sope.etl.utils.WrapperUtil
import org.apache.spark.launcher.{SparkLauncher}

/**
  * @author mbadgujar
  */
object WrapperTest {

  def main(args: Array[String]): Unit = {
      println(args.mkString("\n"))
      WrapperUtil.main(args)

    new SparkLauncher().addJar("")
  }

}
