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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.rendering.QueryRenderer

class AllReduceFallbackTest extends CypherPlannerTestSuite with AstConstructionTestSupport {

  test("allReduce translation to reduce") {
    // allReduce(acc = 0, step IN n | acc + step.x, acc < 11)
    val allReducePred = allReduce(
      accumulator = v"acc",
      init = literalInt(0),
      reductionStepVariable = v"step",
      list = v"n",
      allReduceStepExpression = add(v"acc", prop(v"step", "x")),
      allReducePredicate = lessThan(v"acc", literalInt(11))
    )

    QueryRenderer.pretty(allReducePred) shouldBe "allReduce(acc = 0, step IN n | acc + step.x, acc < 11)"

    val allReduceFallback = allReduceFallBack(
      accumulator = v"acc",
      init = literalInt(0),
      stepVariable = v"step",
      groupVariable = v"n",
      allReduceStepExpression = add(v"acc", prop(v"step", "x")),
      allReducePredicate = lessThan(v"acc", literalInt(11)),
      nextAnonymousVariable = v"  UNNAMED0"
    )

    QueryRenderer.pretty(allReduceFallback) shouldBe
      """reduce(acc = {accumulator: 0, result: true}, step IN n | CASE
        |  WHEN acc.result = false THEN acc
        |  ELSE [`  UNNAMED0` IN [acc.accumulator + step.x] | {accumulator: `  UNNAMED0`, result: acc.result AND `  UNNAMED0` < 11}][0]
        |END).result""".stripMargin

    // Test that allReduce gets correctly translated to reduce with CASE and ListLiteral
    AllReduceFallback(new AnonymousVariableNameGenerator()).rewriteAllReduce(allReducePred) shouldBe allReduceFallback
  }

  test("allReduce translation to reduce - conjunction in allReducePredicate") {
    // allReduce(acc = 1, step IN n | acc * step.x + 1, 1 < acc < 11 AND acc <> 5)
    val allReducePred = allReduce(
      accumulator = v"acc",
      init = literalInt(1),
      reductionStepVariable = v"step",
      list = v"n",
      allReduceStepExpression = add(multiply(v"acc", prop(v"step", "x")), literalInt(1)),
      allReducePredicate =
        ands(lessThan(literalInt(1), v"acc"), lessThan(v"acc", literalInt(11)), notEquals(v"acc", literalInt(5)))
    )

    QueryRenderer.pretty(
      allReducePred
    ) shouldBe "allReduce(acc = 1, step IN n | acc * step.x + 1, 1 < acc AND acc < 11 AND acc <> 5)"

    val allReduceFallback = allReduceFallBack(
      accumulator = v"acc",
      init = literalInt(1),
      stepVariable = v"step",
      groupVariable = v"n",
      allReduceStepExpression = add(multiply(v"acc", prop(v"step", "x")), literalInt(1)),
      allReducePredicate =
        ands(lessThan(literalInt(1), v"acc"), lessThan(v"acc", literalInt(11)), notEquals(v"acc", literalInt(5))),
      nextAnonymousVariable = v"  UNNAMED0"
    )

    QueryRenderer.pretty(allReduceFallback) shouldBe
      """reduce(acc = {accumulator: 1, result: true}, step IN n | CASE
        |  WHEN acc.result = false THEN acc
        |  ELSE [`  UNNAMED0` IN [acc.accumulator * step.x + 1] | {accumulator: `  UNNAMED0`, result: acc.result AND (1 < `  UNNAMED0` AND `  UNNAMED0` < 11 AND `  UNNAMED0` <> 5)}][0]
        |END).result""".stripMargin

    // Test that allReduce gets correctly translated to reduce with CASE and ListLiteral
    AllReduceFallback(new AnonymousVariableNameGenerator()).rewriteAllReduce(allReducePred) shouldBe allReduceFallback
  }

  test("allReduce translation to reduce - step variable in predicate") {
    // allReduce(acc = 1, step IN n | acc * step.x + 1, acc < step.otherProp)
    val allReducePred = allReduce(
      accumulator = v"acc",
      init = literalInt(1),
      reductionStepVariable = v"step",
      list = v"n",
      allReduceStepExpression = add(multiply(v"acc", prop(v"step", "x")), literalInt(1)),
      allReducePredicate = lessThan(v"acc", prop(v"step", "otherProp"))
    )

    QueryRenderer.pretty(
      allReducePred
    ) shouldBe "allReduce(acc = 1, step IN n | acc * step.x + 1, acc < step.otherProp)"

    val allReduceFallback = allReduceFallBack(
      accumulator = v"acc",
      init = literalInt(1),
      stepVariable = v"step",
      groupVariable = v"n",
      allReduceStepExpression = add(multiply(v"acc", prop(v"step", "x")), literalInt(1)),
      allReducePredicate = lessThan(v"acc", prop(v"step", "otherProp")),
      nextAnonymousVariable = v"  UNNAMED0"
    )

    QueryRenderer.pretty(allReduceFallback) shouldBe
      """reduce(acc = {accumulator: 1, result: true}, step IN n | CASE
        |  WHEN acc.result = false THEN acc
        |  ELSE [`  UNNAMED0` IN [acc.accumulator * step.x + 1] | {accumulator: `  UNNAMED0`, result: acc.result AND `  UNNAMED0` < step.otherProp}][0]
        |END).result""".stripMargin

    // Test that allReduce gets correctly translated to reduce with CASE and ListLiteral
    AllReduceFallback(new AnonymousVariableNameGenerator()).rewriteAllReduce(allReducePred) shouldBe allReduceFallback
  }
}
