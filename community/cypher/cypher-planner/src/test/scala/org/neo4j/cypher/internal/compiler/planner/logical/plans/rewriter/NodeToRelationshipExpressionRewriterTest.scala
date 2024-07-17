/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion

class NodeToRelationshipExpressionRewriterTest extends CypherFunSuite with AstConstructionTestSupport {

  private val rel = "rel"

  private val start = "start"

  private val end = "end"

  def rewriterUnderTest: Rewriter =
    NodeToRelationshipExpressionRewriter(varFor(start), varFor(end), varFor(rel)).instance

  def rewrites(inputExpression: Expression, expectedRewrittenExpression: Expression): Assertion = {
    inputExpression.endoRewrite(rewriterUnderTest) should equal(expectedRewrittenExpression)
  }

  def preserves(expr: Expression): Assertion = {
    expr.endoRewrite(rewriterUnderTest) should equal(expr)
  }

  def throws(expr: Expression) = {
    assertThrows[IllegalStateException](expr.endoRewrite(rewriterUnderTest))
  }

  test("should rewrite start.prop <> end.prop") {
    val input = not(equals(prop(start, "prop"), prop(end, "prop")))
    val expectedWrittenOutput =
      not(equals(propExpression(startNode(rel), "prop"), propExpression(endNode(rel), "prop")))
    rewrites(input, expectedWrittenOutput)
  }

  test("should throw on cacheNFromStore[start.prop] == cacheNFromStore[end.prop]") {
    val input = equals(cachedNodePropFromStore(start, "prop"), cachedNodePropFromStore(end, "prop"))
    throws(input)
  }

  test("should throw on cacheNHasProperty[start.prop]") {
    val input = cachedNodeHasProp(start, "prop")
    throws(input)
  }

  test("should rewrite anded property inequalities") {
    val input = andedPropertyInequalities(
      propGreaterThan(start, "prop", 0),
      propLessThan(end, "prop", 10)
    )

    val expectedWrittenOutput = ands(
      greaterThan(prop(startNode(rel), "prop"), literalInt(0)),
      lessThan(prop(endNode(rel), "prop"), literalInt(10))
    )
    rewrites(input, expectedWrittenOutput)
  }

  test("should rewrite boolean expressions") {
    val input = ands(
      or(propGreaterThan(start, "prop", 0), propLessThan(start, "prop", 10)),
      equals(prop(end, "name"), literalString("foo"))
    )

    val expectedWrittenOutput = ands(
      or(
        greaterThan(prop(startNode(rel), "prop"), literalInt(0)),
        lessThan(prop(startNode(rel), "prop"), literalInt(10))
      ),
      equals(prop(endNode(rel), "name"), literalString("foo"))
    )
    rewrites(input, expectedWrittenOutput)
  }

  test("should not rewrite expressions where start and end are not used") {
    preserves(isNotNull(prop(rel, "foo")))
  }

  test("should not rewrite expressions that already use start node and endnode") {
    preserves(not(equals(propExpression(startNode(rel), "prop"), propExpression(endNode(rel), "prop"))))
  }
}
