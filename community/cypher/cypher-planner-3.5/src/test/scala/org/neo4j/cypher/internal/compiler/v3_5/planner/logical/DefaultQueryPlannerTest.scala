/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps.{LogicalPlanProducer, devNullListener}
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, ProvidedOrders, Solveds}
import org.neo4j.cypher.internal.planner.v3_5.spi.{PlanContext, PlanningAttributes}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v3_5.ast.{ASTAnnotationMap, Hint}
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.frontend.phases.devNullLogger
import org.neo4j.cypher.internal.v3_5.util.attribution.IdGen
import org.neo4j.cypher.internal.v3_5.util.symbols._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class DefaultQueryPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("adds ProduceResult with a single node") {
    val result = createProduceResultOperator(Seq("a"), SemanticTable().addNode(varFor("a")))

    result.columns should equal(Seq("a"))
  }

  test("adds ProduceResult with a single relationship") {
    val result = createProduceResultOperator(Seq("r"), SemanticTable().addRelationship(varFor("r")))

    result.columns should equal(Seq("r"))
  }

  test("adds ProduceResult with a single value") {
    val expr = varFor("x")
    val types = ASTAnnotationMap.empty[Expression, ExpressionTypeInfo].updated(expr, ExpressionTypeInfo(CTFloat, None))

    val result = createProduceResultOperator(Seq("x"), semanticTable = SemanticTable(types = types))

    result.columns should equal(Seq("x"))
  }

  private def createProduceResultOperator(columns: Seq[String], semanticTable: SemanticTable): ProduceResult = {
    val solveds = new StubSolveds
    val cardinalities = new StubCardinalities
    val providedOrders = new ProvidedOrders
    val planningContext = mockLogicalPlanningContext(semanticTable, PlanningAttributes(solveds, cardinalities, providedOrders))

    val inputPlan = FakePlan(columns.toSet)

    val queryPlanner = QueryPlanner(planSingleQuery = new FakePlanner(inputPlan))

    val pq = RegularPlannerQuery(horizon = RegularQueryProjection(columns.map(c => c -> varFor(c)).toMap))

    val union = UnionQuery(Seq(pq), distinct = false, columns, periodicCommit = None)

    val (_, result, _) = queryPlanner.plan(union, planningContext, solveds, cardinalities, idGen)

    result shouldBe a [ProduceResult]

    result.asInstanceOf[ProduceResult]
  }

  test("should set strictness when needed") {
    // given
    val plannerQuery = mock[RegularPlannerQuery]
    when(plannerQuery.preferredStrictness).thenReturn(Some(LazyMode))
    when(plannerQuery.queryGraph).thenReturn(QueryGraph.empty)
    when(plannerQuery.lastQueryGraph).thenReturn(QueryGraph.empty)
    when(plannerQuery.horizon).thenReturn(RegularQueryProjection())
    when(plannerQuery.lastQueryHorizon).thenReturn(RegularQueryProjection())
    when(plannerQuery.tail).thenReturn(None)
    when(plannerQuery.allHints).thenReturn(Seq[Hint]())

    val lp = {
      val plan = Argument()
      Projection(plan, Map.empty)
    }

    val context = mock[LogicalPlanningContext]
    val planningAttributes = PlanningAttributes(new Solveds, new Cardinalities, new ProvidedOrders)
    when(context.config).thenReturn(QueryPlannerConfiguration.default)
    when(context.input).thenReturn(QueryGraphSolverInput.empty)
    when(context.planningAttributes).thenReturn(planningAttributes)
    when(context.strategy).thenReturn(new QueryGraphSolver with PatternExpressionSolving {
      override def plan(queryGraph: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): LogicalPlan = {
        context.planningAttributes.solveds.set(lp.id, plannerQuery)
        context.planningAttributes.cardinalities.set(lp.id, 0.0)
        context.planningAttributes.providedOrders.set(lp.id, ProvidedOrder.empty)
        lp
      }
    })
    when(context.costComparisonListener).thenReturn(devNullListener)
    when(context.withStrictness(any())).thenReturn(context)
    when(context.withAddedLeafPlanUpdater(any())).thenReturn(context)
    when(context.withLeafPlanUpdater(any())).thenReturn(context)
    when(context.withUpdatedCardinalityInformation(any())).thenReturn(context)
    val producer = mock[LogicalPlanProducer]
    when(producer.planStarProjection(any(), any(), any())).thenReturn(lp)
    when(producer.planEmptyProjection(any(),any())).thenReturn(lp)
    when(context.logicalPlanProducer).thenReturn(producer)
    val queryPlanner = QueryPlanner(planSingleQuery = PlanSingleQuery())

    // when
    val query = UnionQuery(Seq(plannerQuery), distinct = false, Seq.empty, None)
    queryPlanner.plan(query, context, new Solveds, new Cardinalities, idGen)

    // then
    verify(context, times(1)).withStrictness(LazyMode)
  }

  class FakePlanner(result: LogicalPlan) extends SingleQueryPlanner {
    def apply(input: PlannerQuery, context: LogicalPlanningContext, idGen: IdGen): (LogicalPlan, LogicalPlanningContext) = (result, context)
  }

  private def mockLogicalPlanningContext(semanticTable: SemanticTable, planningAttributes: PlanningAttributes) = LogicalPlanningContext(
    planContext = mock[PlanContext],
    logicalPlanProducer = LogicalPlanProducer(mock[Metrics.CardinalityModel], planningAttributes, idGen),
    metrics = mock[Metrics],
    semanticTable = semanticTable,
    strategy = mock[QueryGraphSolver],
    config = QueryPlannerConfiguration.default,
    notificationLogger = devNullLogger,
    costComparisonListener = devNullListener,
    planningAttributes = planningAttributes)
}
