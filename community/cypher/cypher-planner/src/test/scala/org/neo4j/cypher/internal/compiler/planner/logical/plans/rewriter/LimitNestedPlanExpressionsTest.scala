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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LimitNestedPlanExpressionsTest extends CypherFunSuite with LogicalPlanningTestSupport {
  private val rewriter = limitNestedPlanExpressions(new StubCardinalities, Attributes[LogicalPlan](idGen))

  private val aLit: StringLiteral = StringLiteral("a")(pos.withInputLength(0))

  test("should rewrite Nested plan in Head function") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val nestedPlan = NestedPlanExpression.collect(argument, aLit, aLit)(pos)
    val head = function("head", nestedPlan)

    head.endoRewrite(rewriter) should equal(
      function(
        "head",
        NestedPlanExpression.collect(Limit(argument, SignedDecimalIntegerLiteral("1")(pos)), aLit, aLit)(pos)
      )
    )
  }

  test("should rewrite Nested plan in container index") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val nestedPlan = NestedPlanExpression.collect(argument, aLit, aLit)(pos)
    val ci = ContainerIndex(nestedPlan, SignedDecimalIntegerLiteral("3")(pos))(pos)

    ci.endoRewrite(rewriter) should equal(
      ContainerIndex(
        NestedPlanExpression.collect(
          Limit(argument, Add(SignedDecimalIntegerLiteral("1")(pos), SignedDecimalIntegerLiteral("3")(pos))(pos)),
          aLit,
          aLit
        )(pos),
        SignedDecimalIntegerLiteral("3")(pos)
      )(pos)
    )
  }

  test("should rewrite Nested plan in list slice to") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val nestedPlan = NestedPlanExpression.collect(argument, aLit, aLit)(pos)
    val ls = ListSlice(nestedPlan, None, Some(SignedDecimalIntegerLiteral("4")(pos)))(pos)

    ls.endoRewrite(rewriter) should equal(
      ListSlice(
        NestedPlanExpression.collect(
          Limit(argument, Add(SignedDecimalIntegerLiteral("1")(pos), SignedDecimalIntegerLiteral("4")(pos))(pos)),
          aLit,
          aLit
        )(pos),
        None,
        Some(SignedDecimalIntegerLiteral("4")(pos))
      )(pos)
    )
  }

  test("should rewrite Nested plan in list slice from/to") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val nestedPlan = NestedPlanExpression.collect(argument, aLit, aLit)(pos)
    val ls = ListSlice(
      nestedPlan,
      Some(SignedDecimalIntegerLiteral("2")(pos)),
      Some(SignedDecimalIntegerLiteral("4")(pos))
    )(pos)

    ls.endoRewrite(rewriter) should equal(
      ListSlice(
        NestedPlanExpression.collect(
          Limit(argument, Add(SignedDecimalIntegerLiteral("1")(pos), SignedDecimalIntegerLiteral("4")(pos))(pos)),
          aLit,
          aLit
        )(pos),
        Some(SignedDecimalIntegerLiteral("2")(pos)),
        Some(SignedDecimalIntegerLiteral("4")(pos))
      )(pos)
    )
  }

  test("should rewrite Nested plan in IsEmpty") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val nestedPlan = NestedPlanExpression.collect(argument, aLit, aLit)(pos)
    val isEmpty = function("isEmpty", nestedPlan)

    isEmpty.endoRewrite(rewriter) should equal(
      function(
        "isEmpty",
        NestedPlanExpression.collect(Limit(argument, SignedDecimalIntegerLiteral("1")(pos)), aLit, aLit)(pos)
      )
    )
  }

  test("should not insert Limit on top of Eager in Head function") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val eager = Eager(argument)
    val nestedPlan = NestedPlanExpression.collect(eager, aLit, aLit)(pos)
    val head = function("head", nestedPlan)

    head.endoRewrite(rewriter) should equal(head)
  }

  test("should not insert Limit on top of Top in container index") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val top = Top(argument, Seq(Ascending(v"x")), literalInt(1))
    val nestedPlan = NestedPlanExpression.collect(top, aLit, aLit)(pos)
    val ci = ContainerIndex(nestedPlan, SignedDecimalIntegerLiteral("3")(pos))(pos)

    ci.endoRewrite(rewriter) should equal(ci)
  }

  test("should not insert Limit if container index references variable") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val nestedPlan = NestedPlanExpression.collect(argument, aLit, aLit)(pos)
    val ci = ContainerIndex(nestedPlan, v"x")(pos)

    ci.endoRewrite(rewriter) should equal(ci)
  }

  test("should not insert Limit on top of PartialTop in list slice to") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val partialTop = PartialTop(argument, Seq.empty, Seq.empty, literalInt(1))
    val nestedPlan = NestedPlanExpression.collect(partialTop, aLit, aLit)(pos)
    val ls = ListSlice(nestedPlan, None, Some(SignedDecimalIntegerLiteral("4")(pos)))(pos)

    ls.endoRewrite(rewriter) should equal(ls)
  }

  test("should not insert Limit if list slice to references variable") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val nestedPlan = NestedPlanExpression.collect(argument, aLit, aLit)(pos)
    val ls = ListSlice(nestedPlan, None, Some(v"x"))(pos)

    ls.endoRewrite(rewriter) should equal(ls)
  }

  test("should not insert Limit on top of Eager in list slice from/to") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val eager = Eager(argument)
    val nestedPlan = NestedPlanExpression.collect(eager, aLit, aLit)(pos)
    val ls = ListSlice(
      nestedPlan,
      Some(SignedDecimalIntegerLiteral("2")(pos)),
      Some(SignedDecimalIntegerLiteral("4")(pos))
    )(pos)

    ls.endoRewrite(rewriter) should equal(ls)
  }

  test("should not insert Limit if list slice from/to references variable") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val nestedPlan = NestedPlanExpression.collect(argument, aLit, aLit)(pos)
    val ls = ListSlice(
      nestedPlan,
      Some(SignedDecimalIntegerLiteral("2")(pos)),
      Some(v"x")
    )(pos)

    ls.endoRewrite(rewriter) should equal(ls)
  }
}
