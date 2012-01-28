package org.neo4j.cypher.internal.commands

import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.CypherTypeException

class MathFunctionsTest extends Assertions {
  @Test def shouldWorkOnBothNegativeAndPositiveValues() {
    assert(AbsFunction(Literal(-1))(Map()) === 1)
    assert(AbsFunction(Literal(1))(Map()) === 1)
    intercept[CypherTypeException](AbsFunction(Literal("wut"))(Map()))
  }
}