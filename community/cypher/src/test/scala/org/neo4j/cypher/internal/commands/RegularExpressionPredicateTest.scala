package org.neo4j.cypher.internal.commands

import org.junit.Test
import org.neo4j.cypher.internal.commands.expressions.{Null, Literal}
import org.neo4j.cypher.internal.pipes.QueryStateHelper

class RegularExpressionPredicateTest
{
  @Test def shouldNotMatchIfTheExpressionEvaluatesToNull() {
    val expression: LiteralRegularExpression = new LiteralRegularExpression(Null(), Literal(".*"))
    assert(! expression.isMatch(null)(QueryStateHelper.empty))
  }
}
