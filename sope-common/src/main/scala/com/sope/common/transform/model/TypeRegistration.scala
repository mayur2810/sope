package com.sope.common.transform.model

import com.fasterxml.jackson.databind.jsontype.NamedType

trait TypeRegistration {
   def getTypes: List[NamedType]
}
