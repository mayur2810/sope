package com.sope.common.sql

object Types {

  /*
       D : Dataset
       C : Column
       CF: Column Function
   */

  // Dataset Transformation Function Alias
  type TFunc[D] = D => D
  type TFunc2[D] = D => D => D

  // Join Function Alias, Join type -> Right side dataset -> dataset function
  type JFunc[D] =  String => TFunc2[D]

  // Group Function AliasDSL
  type GFunc[D, C] = Seq[(String, C)] => TFunc[D]

  // Column Functions
  type ColFunc[CF] = CF => CF
  type MultiColFunc[CF] = Seq[CF] => CF
}
