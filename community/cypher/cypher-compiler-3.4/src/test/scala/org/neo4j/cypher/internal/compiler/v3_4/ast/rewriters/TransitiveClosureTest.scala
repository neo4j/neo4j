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
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.{CNFNormalizer, transitiveClosure}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class TransitiveClosureTest extends CypherFunSuite with AstRewritingTestSupport {

  test("MATCH (a)-->(b) WHERE a.prop = b.prop AND b.prop = 42") {
    shouldRewrite(
      "MATCH (a)-->(b) WHERE a.prop = b.prop AND b.prop = 42",
      "MATCH (a)-->(b) WHERE a.prop = 42 AND b.prop = 42")
  }

  test("MATCH (a)-->(b) WHERE b.prop = a.prop AND b.prop = 42") {
    shouldRewrite(
      "MATCH (a)-->(b) WHERE b.prop = a.prop AND b.prop = 42",
      "MATCH (a)-->(b) WHERE b.prop = 42 AND a.prop = 42")
  }

  test("MATCH (a)-->(b) WHERE a.prop = b.prop OR b.prop = 42") {
    shouldNotRewrite("MATCH (a)-->(b) WHERE a.prop = b.prop OR b.prop = 42")
  }

  test("MATCH (a)-->(b) WHERE a.prop = b.prop AND b.prop = b.prop2 AND b.prop2 = 42") {
    shouldRewrite(
      "MATCH (a)-->(b) WHERE a.prop = b.prop AND b.prop = b.prop2 AND b.prop2 = 42",
      "MATCH (a)-->(b) WHERE a.prop = 42 AND b.prop = 42 AND b.prop2 = 42")
  }

  test("MATCH (a)-->(b) WHERE b.prop2 = 42 AND a.prop = b.prop AND b.prop = b.prop2") {
    shouldRewrite(
      "MATCH (a)-->(b) WHERE b.prop2 = 42 AND a.prop = b.prop AND b.prop = b.prop2",
      "MATCH (a)-->(b) WHERE b.prop2 = 42 AND a.prop = 42 AND b.prop = 42")
  }

  test("MATCH (a)-->(b) WHERE (a.prop = b.prop AND b.prop = 42) OR (a.prop = b.prop2 AND b.prop2 = 42)") {
    shouldRewrite(
      "MATCH (a)-->(b) WHERE (a.prop = b.prop AND b.prop = 42) OR (a.prop = b.prop2 AND b.prop2 = 42)",
      "MATCH (a)-->(b) WHERE (a.prop = 42 AND b.prop = 42) OR (a.prop = 42 AND b.prop2 = 42)")
  }

  test("MATCH (a)-->(b) WHERE (a.prop = b.prop AND b.prop = 42) OR (a.prop = b.prop AND b.prop2 = 43) OR (a.prop = b.prop AND b.prop2 = 44)") {
    shouldRewrite(
      "MATCH (a)-->(b) WHERE (a.prop = b.prop AND b.prop = 42) OR (a.prop = b.prop AND b.prop = 43) OR (a.prop = b.prop AND b.prop = 44)",
      "MATCH (a)-->(b) WHERE (a.prop = 42 AND b.prop = 42) OR (a.prop = 43 AND b.prop = 43) OR (a.prop = 44 AND b.prop = 44)")
  }

  private def shouldRewrite(from: String, to: String) {
    val original = parser.parse(from).asInstanceOf[Query]
    val expected = parser.parse(to).asInstanceOf[Query]

    val input = LogicalPlanState(null, null, null, Some(original))
    //We use CNFNormalizer to get it to the canonical form without duplicates
    val result = (transitiveClosure andThen  CNFNormalizer).transform(input, ContextHelper.create())

    //We must also use CNFNormalizer on the expected to get the AND -> ANDS rewrite
    val expectedInput = LogicalPlanState(null, null, null, Some(expected))
    val expectedResult = CNFNormalizer.transform(expectedInput, ContextHelper.create())
    result.statement() should equal(expectedResult.statement())
  }

  private def shouldNotRewrite(q: String) {
    shouldRewrite(q, q)
  }
}
