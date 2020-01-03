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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.v3_5.ast.{Hint, UsingJoinHint}
import org.neo4j.cypher.internal.v3_5.expressions.{PropertyKeyName, SemanticDirection, Variable}
import org.neo4j.cypher.internal.v3_5.logical.plans.{AllNodesScan, CachedNodeProperty, LeftOuterHashJoin, LogicalPlan}
import org.neo4j.cypher.internal.v3_5.util.Cost
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class LeftOuterHashJoinTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val aNode = "a"
  val bNode = "b"
  val cNode = "c"
  val dNode = "d"
  val r1Name = "r1"
  val r2Name = "r2"
  val r3Name = "r3"
  val r1Rel = PatternRelationship(r1Name, (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  val r2Rel = PatternRelationship(r2Name, (bNode, cNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  val r3Rel = PatternRelationship(r3Name, (cNode, dNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  test("solve optional match with left outer join") {
    // MATCH a OPTIONAL MATCH a-->b
    val optionalQg = QueryGraph(
      patternNodes = Set(aNode, bNode),
      patternRelationships = Set(r1Rel),
      argumentIds = Set(aNode)
    )

    val factory = newMockedMetricsFactory

    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case AllNodesScan("b", _) => Cost(1) // Make sure we start the inner plan using b
      case _ => Cost(1000)
    })

    val innerPlan = newMockedLogicalPlan("b")

    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext(),
      metrics = factory.newMetrics(hardcodedStatistics, mock[ExpressionEvaluator], config),
      strategy = newMockedStrategy(innerPlan))
    val left = newMockedLogicalPlanWithPatterns(context.planningAttributes, idNames = Set(aNode))
    val plans = leftOuterHashJoin(optionalQg, left, InterestingOrder.empty, context)

    plans should equal(Some(LeftOuterHashJoin(Set(aNode), left, innerPlan)))
  }

  test("solve optional match with hint") {
    val theHint: Seq[Hint] = Seq(UsingJoinHint(Seq(Variable("a")(pos)))(pos))
    // MATCH a OPTIONAL MATCH a-->b
    val optionalQg = QueryGraph(
      patternNodes = Set(aNode, bNode),
      patternRelationships = Set(r1Rel),
      hints = theHint,
      argumentIds = Set(aNode)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case AllNodesScan("b", _) => Cost(1) // Make sure we start the inner plan using b
      case _ => Cost(1000)
    })

    val innerPlan = newMockedLogicalPlan("b")

    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext(),
      metrics = factory.newMetrics(hardcodedStatistics, mock[ExpressionEvaluator], config),
      strategy = newMockedStrategy(innerPlan))
    val left = newMockedLogicalPlanWithPatterns(context.planningAttributes, Set(aNode))
    val plan = leftOuterHashJoin(optionalQg, left, InterestingOrder.empty, context).getOrElse(fail("No result from outerHashJoin"))

    plan should equal(LeftOuterHashJoin(Set(aNode), left, innerPlan))
    context.planningAttributes.solveds.get(plan.id).lastQueryGraph.allHints should equal (theHint)
  }

  test("should not expose cached node properties from rhs where node is join key") {
    def cachedProp(node: String, propertyKey: String) =
      prop(node, propertyKey) -> CachedNodeProperty(node, PropertyKeyName(propertyKey)(pos))(pos)

    // given
    val lhs = mock[LogicalPlan]
    when(lhs.availableSymbols).thenReturn(Set("a", "b"))
    when(lhs.availableCachedNodeProperties).thenReturn(Map(cachedProp("a", "lhs"), cachedProp("b", "lhs")))
    val rhs = mock[LogicalPlan]
    when(rhs.availableSymbols).thenReturn(Set("b", "c"))
    when(rhs.availableCachedNodeProperties).thenReturn(Map(cachedProp("b", "rhs"), cachedProp("c", "rhs")))
    val join = LeftOuterHashJoin(Set("b"), lhs, rhs)

    // then
    join.availableCachedNodeProperties should be(Map(
      cachedProp("a", "lhs"),
      cachedProp("b", "lhs"),
      cachedProp("c", "rhs")
    ))
  }
}
