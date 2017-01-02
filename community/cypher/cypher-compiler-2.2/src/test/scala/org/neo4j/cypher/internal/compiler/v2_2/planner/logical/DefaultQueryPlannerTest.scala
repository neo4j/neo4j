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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.mockito.Answers
import org.mockito.Matchers.any
import org.mockito.Mockito.{times, verify, when}
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v2_2.{Rewriter, InputPosition}
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{Projection, IdName, SingleRow, LazyMode, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v2_2.planner.{RegularQueryProjection, CardinalityEstimation, PlannerQuery, UnionQuery, LogicalPlanningTestSupport2, QueryGraph, SemanticTable}

class DefaultQueryPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("names unnamed patterns when planning pattern expressions") {

    // given
    val expectedPlan = mock[LogicalPlan]
    val solver = new TentativeQueryGraphSolver {
      override def config = ???
      override def tryPlan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): Option[LogicalPlan] = ???
      override def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = expectedPlan
    }

    implicit val context = LogicalPlanningContext(null, LogicalPlanProducer(mock[Metrics.CardinalityModel]), null, SemanticTable(), solver)
    val patExpr = parsePatternExpression("WITH {a} AS a, {r} AS r, {b} AS b LIMIT 1 RETURN ()-[]->(b)")

    // when
    val (producedPlan, namedExpr) = solver.planPatternExpression(Set.empty, patExpr)

    // then
    val expectedExpr = parsePatternExpression("WITH {a} AS a, {r} AS r, {b} AS b LIMIT 1 RETURN (`  UNNAMED50`)-[`  UNNAMED52`]->(b)")

    expectedExpr should equal(namedExpr)
    expectedPlan should equal(producedPlan)
  }

  test("build correct semantic table when planning pattern expressions") {

    // given
    val expectedPlan = mock[LogicalPlan]
    val solver = new QueryGraphSolver with PatternExpressionSolving {
      override def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = {
        val table = context.semanticTable
        table.isNode(Identifier("  UNNAMED50")(InputPosition(49, 1, 50))) should be(true)
        table.isRelationship(Identifier("  UNNAMED52")(InputPosition(51, 1, 52))) should be(true)
        expectedPlan
      }
    }

    implicit val context = LogicalPlanningContext(null, LogicalPlanProducer(mock[Metrics.CardinalityModel]), null, SemanticTable(), solver)
    val patExpr = parsePatternExpression("WITH {a} AS a, {r} AS r, {b} AS b LIMIT 1 RETURN ()-[]->(b)")

    // when
    val (producedPlan, _) = solver.planPatternExpression(Set.empty, patExpr)

    // then
    expectedPlan should equal(producedPlan)
  }

  test("should set strictness when needed") {
    // given
    val plannerQuery = mock[PlannerQuery with CardinalityEstimation]
    when(plannerQuery.preferredStrictness).thenReturn(Some(LazyMode))
    when(plannerQuery.graph).thenReturn(QueryGraph.empty)
    when(plannerQuery.horizon).thenReturn(RegularQueryProjection())
    when(plannerQuery.tail).thenReturn(None)

    val lp = {
      val plan = mock[Projection]
      when(plan.availableSymbols).thenReturn(Set.empty[IdName])
      when(plan.solved).thenReturn(plannerQuery)
      plan
    }

    val context = mock[LogicalPlanningContext]
    when(context.input).thenReturn(QueryGraphSolverInput.empty)
    when(context.strategy).thenReturn(new QueryGraphSolver with PatternExpressionSolving {
      override def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = lp
    })
    when(context.withStrictness(any())).thenReturn(context)
    val producer = mock[LogicalPlanProducer]
    when(producer.planStarProjection(any(), any())(any())).thenReturn(lp)
    when(context.logicalPlanProducer).thenReturn(producer)
    val queryPlanner = new DefaultQueryPlanner(planRewriter = Rewriter.noop, expressionRewriterFactory = (lpc) => Rewriter.noop )

    // when
    val query = UnionQuery(Seq(plannerQuery), distinct = false)
    queryPlanner.plan(query)(context)

    // then
    verify(context, times(1)).withStrictness(LazyMode)
  }

  private def parsePatternExpression(query: String): PatternExpression = {
    parser.parse(query) match {
      case Query(_, SingleQuery(clauses)) =>
        val ret = clauses.last.asInstanceOf[Return]
        val patExpr = ret.returnItems.items.head.expression.asInstanceOf[PatternExpression]
        patExpr
    }
  }
}
