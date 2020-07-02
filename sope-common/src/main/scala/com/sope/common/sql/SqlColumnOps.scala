package com.sope.common.sql

import com.sope.common.sql.Types.{ColFunc, MultiColFunc}

/**
 * @author mbadgujar
 */
trait SqlColumnOps[CF] {
  def resolveSingleArgFunction(functionName: String): ColFunc[CF]

  def resolveMultiArgFunction(functionName: String): MultiColFunc[CF]
}
