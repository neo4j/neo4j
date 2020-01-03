/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.v3_5.expressions.{Add, ContainerIndex, ListSlice, SignedDecimalIntegerLiteral, StringLiteral}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class LimitNestedPlanExpressionsTest extends CypherFunSuite with LogicalPlanningTestSupport {
  private val rewriter = limitNestedPlanExpressions(idGen)

  test("should rewrite Nested plan in Head function") {
    val argument: LogicalPlan = Argument(Set("a"))
    val nestedPlan = NestedPlanExpression(argument, StringLiteral("a")(pos))(pos)
    val head = function("head", nestedPlan)

    head.endoRewrite(rewriter) should equal(
      function("head", NestedPlanExpression(Limit(argument, SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies), StringLiteral("a")(pos))(pos))
    )
  }

  test("should rewrite Nested plan in container index") {
    val argument: LogicalPlan = Argument(Set("a"))
    val nestedPlan = NestedPlanExpression(argument, StringLiteral("a")(pos))(pos)
    val ci = ContainerIndex(nestedPlan, SignedDecimalIntegerLiteral("3")(pos))(pos)

    ci.endoRewrite(rewriter) should equal(
      ContainerIndex(NestedPlanExpression(Limit(argument, Add(SignedDecimalIntegerLiteral("1")(pos), SignedDecimalIntegerLiteral("3")(pos))(pos), DoNotIncludeTies),
        StringLiteral("a")(pos))(pos), SignedDecimalIntegerLiteral("3")(pos))(pos)
    )
  }

  test("should rewrite Nested plan in list slice to") {
    val argument: LogicalPlan = Argument(Set("a"))
    val nestedPlan = NestedPlanExpression(argument, StringLiteral("a")(pos))(pos)
    val ls = ListSlice(nestedPlan, None, Some(SignedDecimalIntegerLiteral("4")(pos)))(pos)

    ls.endoRewrite(rewriter) should equal(
      ListSlice(NestedPlanExpression(Limit(argument, Add(SignedDecimalIntegerLiteral("1")(pos), SignedDecimalIntegerLiteral("4")(pos))(pos), DoNotIncludeTies),
        StringLiteral("a")(pos))(pos), None, Some(SignedDecimalIntegerLiteral("4")(pos)))(pos)
    )
  }

  test("should rewrite Nested plan in list slice from/to") {
    val argument: LogicalPlan = Argument(Set("a"))
    val nestedPlan = NestedPlanExpression(argument, StringLiteral("a")(pos))(pos)
    val ls = ListSlice(nestedPlan, Some(SignedDecimalIntegerLiteral("2")(pos)), Some(SignedDecimalIntegerLiteral("4")(pos)))(pos)

    ls.endoRewrite(rewriter) should equal(
      ListSlice(NestedPlanExpression(Limit(argument, Add(SignedDecimalIntegerLiteral("1")(pos), SignedDecimalIntegerLiteral("4")(pos))(pos), DoNotIncludeTies),
        StringLiteral("a")(pos))(pos), Some(SignedDecimalIntegerLiteral("2")(pos)), Some(SignedDecimalIntegerLiteral("4")(pos)))(pos)
    )
  }
}
