/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v3_4.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_4.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_4.planner.AstRewritingTestSupport
import org.neo4j.cypher.internal.compiler.v3_4.test_helpers.ContextHelper
import org.neo4j.cypher.internal.frontend.v3_4.ast.Query
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.rewriteEqualityToInPredicate
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite

class RewriteEqualityToInPredicateTest extends CypherFunSuite with AstRewritingTestSupport {

  test("MATCH (a) WHERE id(a) = 42 (no dependencies on the RHS)") {
    shouldRewrite(
      "MATCH (a) WHERE id(a) = 42",
      "MATCH (a) WHERE id(a) IN [42]")
  }

  test("MATCH (a) WHERE a.prop = 42 (no dependencies on the RHS)") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop = 42",
      "MATCH (a) WHERE a.prop IN [42]")
  }

  test("MATCH (a) WHERE id(a) = rand() (no dependencies on the RHS)") {
    shouldRewrite(
      "MATCH (a) WHERE id(a) = rand()",
      "MATCH (a) WHERE id(a) IN [rand()]")
  }

  test("MATCH (a) WHERE a.prop = rand() (no dependencies on the RHS)") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop = rand()",
      "MATCH (a) WHERE a.prop IN [rand()]")
  }

  test("WITH x as 42 MATCH (a) WHERE id(a) = x (no dependencies on the RHS)") {
    shouldRewrite(
      "WITH 42 as x MATCH (a) WHERE id(a) = x",
      "WITH 42 as x MATCH (a) WHERE id(a) IN [x]")
  }

  test("WITH x as 42 MATCH (a) WHERE a.prop = x (no dependencies on the RHS)") {
    shouldRewrite(
      "WITH 42 as x MATCH (a) WHERE a.prop = x",
      "WITH 42 as x MATCH (a) WHERE a.prop IN [x]")
  }

  test("should not rewrite a comparison between two properties") {
    shouldNotRewrite(
      "MATCH (a), (b) WHERE a.prop = b.prop")
  }

  private def shouldRewrite(from: String, to: String) {
    val original = parser.parse(from).asInstanceOf[Query]
    val expected = parser.parse(to).asInstanceOf[Query]

    val input = LogicalPlanState(null, null, null, Some(original))
    val result = rewriteEqualityToInPredicate.transform(input, ContextHelper.create())

    result.statement should equal(expected)
  }

  private def shouldNotRewrite(q: String) {
    shouldRewrite(q, q)
  }
}
