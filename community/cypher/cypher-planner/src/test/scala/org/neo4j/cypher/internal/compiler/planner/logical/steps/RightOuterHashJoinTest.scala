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

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.planner._
import org.neo4j.cypher.internal.compiler.planner.logical.{ExpressionEvaluator, PlanMatchHelp}
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.ir.{PatternRelationship, QueryGraph, SimplePatternLength, InterestingOrder}
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.logical.plans.{AllNodesScan, LogicalPlan, RightOuterHashJoin}
import org.neo4j.cypher.internal.v4_0.ast.{Hint, UsingJoinHint}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.Cost
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class RightOuterHashJoinTest extends CypherFunSuite with LogicalPlanningTestSupport with PlanMatchHelp {

  private val aNode = "a"
  private val bNode = "b"
  private val r1Rel = PatternRelationship("r1", (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  test("solve optional match with right outer join") {
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
    val plans = rightOuterHashJoin(optionalQg, left, InterestingOrder.empty, context)

    plans should equal(Some(RightOuterHashJoin(Set(aNode), innerPlan, left)))
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
    val plan = rightOuterHashJoin(optionalQg, left, InterestingOrder.empty, context).getOrElse(fail("No result from outerHashJoin"))

    plan should equal(RightOuterHashJoin(Set(aNode), innerPlan, left))
    context.planningAttributes.solveds.get(plan.id).asSinglePlannerQuery.lastQueryGraph.allHints should equal (theHint)
  }

}
