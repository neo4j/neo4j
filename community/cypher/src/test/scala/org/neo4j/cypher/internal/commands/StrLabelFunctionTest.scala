package org.neo4j.cypher.internal.commands

import expressions.{LabelValue, Literal, StrLabelFunction}
import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.CypherTypeException

/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */

class StrLabelFunctionTest extends Assertions {

  @Test
  def testWithCorrectType() {
    assert(StrLabelFunction(Literal("bluey"))(ExecutionContext.empty) === LabelValue("bluey"))
  }

  @Test
  def testWithIncorrectType() {
    intercept[CypherTypeException] { StrLabelFunction(Literal(3))(ExecutionContext.empty) }
  }

  @Test
  def testCalculateTypeThrows() {
    intercept[CypherTypeException] { StrLabelFunction(Literal(3)).calculateType(null) }
  }
}