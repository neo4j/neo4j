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
package org.neo4j.cypher.internal.planning

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.simpleExpressionEvaluator
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class simpleExpressionEvaluatorTest extends CypherFunSuite with AstConstructionTestSupport {

  private val randInvocation: FunctionInvocation =
    FunctionInvocation(FunctionName("ranD")(pos), distinct = false, IndexedSeq.empty)(pos)

  test("isNonDeterministic should not care about capitalization") {
    val evaluator = simpleExpressionEvaluator
    evaluator.isDeterministic(randInvocation) shouldBe false
  }

  test("evaluateLongIfStable on IntegerLiteral") {
    simpleExpressionEvaluator.evaluateLongIfStable(literalInt(5)) should be(Some(5))
  }

  test("evaluateLongIfStable on non-deterministic expression") {
    simpleExpressionEvaluator.evaluateLongIfStable(randInvocation) should be(None)
  }

  test("evaluateLongIfStable on expression with parameters") {
    simpleExpressionEvaluator.evaluateLongIfStable(add(literalInt(5), parameter("p", CTInteger))) should be(None)
  }

  test("evaluateLongIfStable on int expression") {
    simpleExpressionEvaluator.evaluateLongIfStable(add(literalInt(5), literalInt(6))) should be(Some(11))
  }

  test("evaluateLongIfStable on string expression") {
    simpleExpressionEvaluator.evaluateLongIfStable(literalString("foo")) should be(None)
  }
}
