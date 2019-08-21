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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.neo4j.cypher.internal.compiler.planner._
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.steps.{LogicalPlanProducer, devNullListener}
import org.neo4j.cypher.internal.ir._
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.{Cardinalities, ProvidedOrders, Solveds}
import org.neo4j.cypher.internal.planner.spi.{PlanContext, PlanningAttributes}
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.ast.{ASTAnnotationMap, Hint}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.frontend.phases.devNullLogger
import org.neo4j.cypher.internal.logical.plans.{Argument, LogicalPlan, ProduceResult, Projection}
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

// TODO fix tests in this class
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

    val singleQuery = RegularSinglePlannerQuery(horizon = RegularQueryProjection(columns.map(c => c -> varFor(c)).toMap))

    val plannerQuery = PlannerQuery(singleQuery, periodicCommit = None)

    val (_, result, _) = queryPlanner.plan(plannerQuery, planningContext, columns)

    result shouldBe a [ProduceResult]

    result.asInstanceOf[ProduceResult]
  }

  test("should set strictness when needed") {
    // given
    val singlePlannerQuery = mock[RegularSinglePlannerQuery]
    when(singlePlannerQuery.preferredStrictness).thenReturn(Some(LazyMode))
    when(singlePlannerQuery.queryGraph).thenReturn(QueryGraph.empty)
    when(singlePlannerQuery.lastQueryGraph).thenReturn(QueryGraph.empty)
    when(singlePlannerQuery.horizon).thenReturn(RegularQueryProjection())
    when(singlePlannerQuery.lastQueryHorizon).thenReturn(RegularQueryProjection())
    when(singlePlannerQuery.tail).thenReturn(None)
    when(singlePlannerQuery.allHints).thenReturn(Seq[Hint]())
    when(singlePlannerQuery.interestingOrder).thenReturn(InterestingOrder.empty)
    when(singlePlannerQuery.queryInput).thenReturn(None)

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
        context.planningAttributes.solveds.set(lp.id, singlePlannerQuery)
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
    val plannerQuery = PlannerQuery(singlePlannerQuery, periodicCommit = None)
    queryPlanner.plan(plannerQuery, context, Seq.empty)

    // then
    verify(context).withStrictness(LazyMode)
  }

  class FakePlanner(result: LogicalPlan) extends SingleQueryPlanner {
    def apply(input: SinglePlannerQuery, context: LogicalPlanningContext): (LogicalPlan, LogicalPlanningContext) = (result, context)
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
    planningAttributes = planningAttributes,
    innerVariableNamer = innerVariableNamer,
    idGen = idGen)
}
