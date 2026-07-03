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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RewrittenExpressionsTest extends CypherFunSuite with LogicalPlanAstConstructionTestSupport {

  test("RewrittenExpressions.empty should use an empty map") {
    RewrittenExpressions.empty shouldBe empty
    RewrittenExpressions.empty.isEmpty shouldBe true
  }

  test("rewrittenExpressionOrSelf should get rewritten expression if it exists") {
    val rewrittenExpressions = RewrittenExpressions.forMap(Map(prop("a", "foo") -> cachedNodeProp("a", "foo")))
    rewrittenExpressions.rewrittenExpressionOrSelf(prop("a", "foo")) shouldEqual cachedNodeProp("a", "foo")
  }

  test("rewrittenExpressionOrSelf should return the original expression if no rewrittenExpressionExists") {
    val rewrittenExpressions = RewrittenExpressions.forMap(Map(prop("a", "foo") -> cachedNodeProp("a", "foo")))
    rewrittenExpressions.rewrittenExpressionOrSelf(prop("b", "bar")) shouldEqual prop("b", "bar")
  }

  test("allRewrittenExpressions should return all the rewritten expressions from the map") {
    val rewrittenExpressions = RewrittenExpressions.forMap(Map(
      prop("a", "foo") -> cachedNodeProp("a", "foo"),
      prop("b", "bar") -> cachedNodeProp("b", "bar")
    ))
    rewrittenExpressions.allRewrittenExpressions.toSet shouldEqual Set(
      cachedNodeProp("a", "foo"),
      cachedNodeProp("b", "bar")
    )
  }

  test("withNoRewrittenExprs should store a map of the expressions to themselves") {
    val exprs = Set(prop("a", "foo"), prop("b", "bar"))
    val rewrittenExpressions = RewrittenExpressions.withNoRewrittenExprs(exprs)
    rewrittenExpressions.allRewrittenExpressions.toSet shouldEqual exprs
  }
}
