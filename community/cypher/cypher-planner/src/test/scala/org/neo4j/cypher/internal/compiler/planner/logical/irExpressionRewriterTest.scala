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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.IRExpression
import org.neo4j.cypher.internal.ir.ast.ListIRExpression
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class irExpressionRewriterTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  private val dummyPlan = AllNodesScan(v"a", Set.empty)

  case class TestableIRExpression(expression: IRExpression, expectedRewrite: Expression)

  private val listIrExpr1 = {
    val expr = newListIrExpression(Set("a"), Set("a", "b"))
    val plan = new LogicalPlanBuilder(wholePlan = false)
      .cartesianProduct()
      .|.allNodeScan("b", "a")
      .argument("a")
      .build()
    TestableIRExpression(
      expr,
      NestedPlanExpression.collect(
        plan,
        expr.variableToCollect,
        varFor(expr.solvedExpressionAsString)
      )(pos)
    )
  }

  private val listIrExpr2 = {
    val expr = newListIrExpression(Set("a"), Set("a", "c"))
    val plan = new LogicalPlanBuilder(wholePlan = false)
      .cartesianProduct()
      .|.allNodeScan("c", "a")
      .argument("a")
      .build()
    TestableIRExpression(
      expr,
      NestedPlanExpression.collect(
        plan,
        expr.variableToCollect,
        varFor(expr.solvedExpressionAsString)
      )(pos)
    )
  }

  private val existsIrExpr = {
    val expr = newExistsIrExpression(Set("a"), Set("a", "b"))
    val plan = new LogicalPlanBuilder(wholePlan = false)
      .cartesianProduct()
      .|.allNodeScan("b", "a")
      .argument("a")
      .build()
    TestableIRExpression(
      expr,
      NestedPlanExpression.exists(
        plan,
        varFor(expr.solvedExpressionAsString)
      )(pos)
    )
  }

  test("Rewrites single ListIRExpression") {
    // given
    val TestableIRExpression(expr, expected) = listIrExpr1

    new givenConfig().withLogicalPlanningContextWithFakeAttributes { (_, context) =>
      val rewriter = irExpressionRewriter(dummyPlan, context)
      // when
      val result = rewriter(expr)

      // then
      result should equal(expected)
    }
  }

  test("Rewrites single ExistsIRExpression") {
    // given
    val TestableIRExpression(expr, expected) = existsIrExpr

    new givenConfig().withLogicalPlanningContextWithFakeAttributes { (_, context) =>
      val rewriter = irExpressionRewriter(dummyPlan, context)
      // when
      val result = rewriter(expr)

      // then
      result should equal(expected)
    }
  }

  test("Rewrites two anded ListIRExpressions") {
    val TestableIRExpression(expr1, expected1) = listIrExpr1
    val TestableIRExpression(expr2, expected2) = listIrExpr2
    val expr = and(expr1, expr2)

    new givenConfig().withLogicalPlanningContextWithFakeAttributes { (_, context) =>
      val rewriter = irExpressionRewriter(dummyPlan, context)
      // when
      val result = rewriter(expr)

      // then
      result should equal(
        and(expected1, expected2)
      )
    }
  }

  test("Does not rewrite IR expressions on nested plans") {
    // given
    val expr = NestedPlanExpression.collect(dummyPlan, listIrExpr1.expression, listIrExpr2.expression)(pos)

    new givenConfig().withLogicalPlanningContextWithFakeAttributes { (_, context) =>
      val rewriter = irExpressionRewriter(dummyPlan, context)
      // when
      val result = rewriter(expr)

      // then
      result should equal(expr)
    }
  }

  test("Does not rewrite pattern expressions inside nested plans") {
    // given
    val plan = new LogicalPlanBuilder(wholePlan = false)
      .projection(Map("p" -> listIrExpr1.expression))
      .argument()
      .build()
    val expr = NestedPlanExpression.collect(plan, v"x", v"y")(pos)

    new givenConfig().withLogicalPlanningContextWithFakeAttributes { (_, context) =>
      val rewriter = irExpressionRewriter(dummyPlan, context)
      // when
      val result = rewriter(expr)

      // then
      result should equal(expr)
    }
  }

  private def newListIrExpression(argumentIds: Set[String], patternNodes: Set[String]): ListIRExpression = {
    ListIRExpression(
      RegularSinglePlannerQuery(
        QueryGraph(
          argumentIds = argumentIds.map(varFor),
          patternNodes = patternNodes.map(varFor)
        )
      ),
      v"anon_0",
      v"anon_1",
      "ListIRExpression"
    )(pos, None, Some(argumentIds.map(name => varFor(name))))
  }

  private def newExistsIrExpression(argumentIds: Set[String], patternNodes: Set[String]): ExistsIRExpression = {
    ExistsIRExpression(
      RegularSinglePlannerQuery(
        QueryGraph(
          argumentIds = argumentIds.map(varFor),
          patternNodes = patternNodes.map(varFor)
        )
      ),
      v"anon_0",
      "ExistsIRExpression"
    )(pos, None, Some(argumentIds.map(name => varFor(name))))
  }
}
