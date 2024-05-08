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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.CostModelMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.MetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.simpleExpressionEvaluator
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class OuterHashJoinTest extends CypherFunSuite with LogicalPlanningTestSupport with PlanMatchHelp {

  private val planContext = notImplementedPlanContext(hardcodedStatistics)

  private val aNode = v"a"
  private val bNode = v"b"
  private val r1Var = v"r1"

  private val r1Rel =
    PatternRelationship(r1Var, (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  private def newMockedMetrics(factory: MetricsFactory): Metrics =
    factory.newMetrics(
      planContext,
      simpleExpressionEvaluator,
      ExecutionModel.default,
      CancellationChecker.neverCancelled()
    )

  test("solve optional match with outer joins") {
    // MATCH a OPTIONAL MATCH a-->b
    val optionalQg = QueryGraph(
      patternNodes = Set(aNode, bNode),
      patternRelationships = Set(r1Rel),
      argumentIds = Set(aNode)
    )
    val enclosingQg = QueryGraph(optionalMatches = IndexedSeq(optionalQg))

    val factory = newMockedMetricsFactory

    when(factory.newCostModel(ExecutionModel.default, CancellationChecker.neverCancelled())).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case AllNodesScan(`bNode`, _) => Cost(1) // Make sure we start the inner plan using b
          case _                        => Cost(1000)
        }
    ): CostModel)

    val innerPlan = newMockedLogicalPlan(bNode.name)

    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext(),
      metrics = newMockedMetrics(factory),
      strategy = newMockedStrategy(innerPlan)
    )
    val left = newMockedLogicalPlanWithPatterns(context.staticComponents.planningAttributes, idNames = Set(aNode.name))
    val plans = outerHashJoin.solver(optionalQg, enclosingQg, InterestingOrderConfig.empty, context).connect(left).toSeq

    plans should contain theSameElementsAs Seq(
      LeftOuterHashJoin(Set(aNode), left, innerPlan),
      RightOuterHashJoin(Set(aNode), innerPlan, left)
    )
  }

  test("solve optional match with hint") {
    val theHint: Set[Hint] = Set(UsingJoinHint(Seq(aNode))(pos))
    // MATCH a OPTIONAL MATCH a-->b
    val optionalQg = QueryGraph(
      patternNodes = Set(aNode, bNode),
      patternRelationships = Set(r1Rel),
      hints = theHint,
      argumentIds = Set(aNode)
    )
    val enclosingQg = QueryGraph(optionalMatches = IndexedSeq(optionalQg))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default, CancellationChecker.neverCancelled())).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case AllNodesScan(`bNode`, _) => Cost(1) // Make sure we start the inner plan using b
          case _                        => Cost(1000)
        }
    ): CostModel)

    val innerPlan = newMockedLogicalPlan(bNode.name)

    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext(),
      metrics = newMockedMetrics(factory),
      strategy = newMockedStrategy(innerPlan)
    )
    val left = newMockedLogicalPlanWithPatterns(context.staticComponents.planningAttributes, Set(aNode.name))
    val plans = outerHashJoin.solver(optionalQg, enclosingQg, InterestingOrderConfig.empty, context).connect(left).toSeq

    plans should contain theSameElementsAs Seq(
      LeftOuterHashJoin(Set(aNode), left, innerPlan),
      RightOuterHashJoin(Set(aNode), innerPlan, left)
    )
    plans.map { p =>
      context.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.lastQueryGraph.allHints
    } foreach {
      _ should equal(theHint)
    }
  }

  test("solve optional match with interesting order with outer joins") {
    // MATCH a OPTIONAL MATCH a-->b
    val optionalQg = QueryGraph(
      patternNodes = Set(aNode, bNode),
      patternRelationships = Set(r1Rel),
      argumentIds = Set(aNode)
    )
    val enclosingQg = QueryGraph(optionalMatches = IndexedSeq(optionalQg))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default, CancellationChecker.neverCancelled())).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case AllNodesScan(`bNode`, _) => Cost(1) // Make sure we start the inner plan using b
          case _                        => Cost(1000)
        }
    ): CostModel)

    val unorderedPlan = newMockedLogicalPlan(bNode.name, "iAmUnordered")
    val orderedPlan = newMockedLogicalPlan(bNode.name, "iAmOrdered")

    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext(),
      metrics = newMockedMetrics(factory),
      strategy = newMockedStrategyWithSortedPlan(unorderedPlan, orderedPlan)
    )
    val left = newMockedLogicalPlanWithPatterns(context.staticComponents.planningAttributes, idNames = Set(aNode.name))
    val io = InterestingOrderConfig(InterestingOrder.required(RequiredOrderCandidate.asc(bNode)))
    val plans = outerHashJoin.solver(optionalQg, enclosingQg, io, context).connect(left).toSeq

    plans should contain theSameElementsAs Seq(
      LeftOuterHashJoin(Set(aNode), left, unorderedPlan),
      LeftOuterHashJoin(Set(aNode), left, orderedPlan),
      RightOuterHashJoin(Set(aNode), unorderedPlan, left)
    )
  }
}
