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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.PlanMatchHelp
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
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class OuterHashJoinTest extends CypherFunSuite with LogicalPlanningTestSupport with PlanMatchHelp {

  private val aNode = "a"
  private val bNode = "b"
  private val r1Name = "r1"
  private val r1Rel = PatternRelationship(r1Name, (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  test("solve optional match with outer joins") {
    // MATCH a OPTIONAL MATCH a-->b
    val optionalQg = QueryGraph(
      patternNodes = Set(aNode, bNode),
      patternRelationships = Set(r1Rel),
      argumentIds = Set(aNode)
    )

    val factory = newMockedMetricsFactory

    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case AllNodesScan(`bNode`, _) => Cost(1) // Make sure we start the inner plan using b
      case _ => Cost(1000)
    })

    val innerPlan = newMockedLogicalPlan(bNode)

    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext(),
      metrics = factory.newMetrics(hardcodedStatistics, mock[ExpressionEvaluator], config),
      strategy = newMockedStrategy(innerPlan))
    val left = newMockedLogicalPlanWithPatterns(context.planningAttributes, idNames = Set(aNode))
    val plans = outerHashJoin(optionalQg, left, InterestingOrder.empty, context).toSeq

    plans should contain theSameElementsAs Seq(
      LeftOuterHashJoin(Set(aNode), left, innerPlan),
      RightOuterHashJoin(Set(aNode), innerPlan, left),
    )
  }

  test("solve optional match with hint") {
    val theHint: Set[Hint] = Set(UsingJoinHint(Seq(varFor(aNode)))(pos))
    // MATCH a OPTIONAL MATCH a-->b
    val optionalQg = QueryGraph(
      patternNodes = Set(aNode, bNode),
      patternRelationships = Set(r1Rel),
      hints = theHint,
      argumentIds = Set(aNode)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case AllNodesScan(`bNode`, _) => Cost(1) // Make sure we start the inner plan using b
      case _ => Cost(1000)
    })

    val innerPlan = newMockedLogicalPlan(bNode)

    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext(),
      metrics = factory.newMetrics(hardcodedStatistics, mock[ExpressionEvaluator], config),
      strategy = newMockedStrategy(innerPlan))
    val left = newMockedLogicalPlanWithPatterns(context.planningAttributes, Set(aNode))
    val plans = outerHashJoin(optionalQg, left, InterestingOrder.empty, context).toSeq

    plans should contain theSameElementsAs Seq(
      LeftOuterHashJoin(Set(aNode), left, innerPlan),
      RightOuterHashJoin(Set(aNode), innerPlan, left),
    )
    plans.map { p=>
      context.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.lastQueryGraph.allHints
    } foreach {
      _ should equal (theHint)
    }
  }

  test("solve optional match with interesting order with outer joins") {
    // MATCH a OPTIONAL MATCH a-->b
    val optionalQg = QueryGraph(
      patternNodes = Set(aNode, bNode),
      patternRelationships = Set(r1Rel),
      argumentIds = Set(aNode)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case AllNodesScan(`bNode`, _) => Cost(1) // Make sure we start the inner plan using b
      case _ => Cost(1000)
    })

    val unorderedPlan = newMockedLogicalPlan(bNode, "iAmUnordered")
    val orderedPlan = newMockedLogicalPlan(bNode, "iAmOrdered")

    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext(),
      metrics = factory.newMetrics(hardcodedStatistics, mock[ExpressionEvaluator], config),
      strategy = newMockedStrategyWithSortedPlan(unorderedPlan, orderedPlan))
    val left = newMockedLogicalPlanWithPatterns(context.planningAttributes, idNames = Set(aNode))
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor(bNode)))
    val plans = outerHashJoin(optionalQg, left, io, context).toSeq

    plans should contain theSameElementsAs Seq(
      LeftOuterHashJoin(Set(aNode), left, unorderedPlan),
      LeftOuterHashJoin(Set(aNode), left, orderedPlan),
      RightOuterHashJoin(Set(aNode), unorderedPlan, left),
    )
  }
}
