/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.steps

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_3.planner._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Hint, PatternExpression, UsingJoinHint, Variable}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_3._
import org.neo4j.cypher.internal.v3_3.logical.plans.{AllNodesScan, LogicalPlan, OuterHashJoin}

class OuterHashJoinTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

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

  test("solve optional match with outer join") {
    // MATCH a OPTIONAL MATCH a-->b
    val optionalQg = QueryGraph(
      patternNodes = Set(aNode, bNode),
      patternRelationships = Set(r1Rel),
      argumentIds = Set(aNode)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => plan match {
      case AllNodesScan("b", _) => Cost(1) // Make sure we start the inner plan using b
      case _ => Cost(1000)
    })

    val innerPlan = newMockedLogicalPlan("b")

    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      strategy = newMockedStrategy(innerPlan),
      metrics = factory.newMetrics(hardcodedStatistics, mock[ExpressionEvaluator], config)
    )
    val left = newMockedLogicalPlanWithPatterns(Set(aNode))
    val plans = outerHashJoin(optionalQg, left)

    plans should equal(Some(OuterHashJoin(Set(aNode), left, innerPlan)(solved)))
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
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => plan match {
      case AllNodesScan("b", _) => Cost(1) // Make sure we start the inner plan using b
      case _ => Cost(1000)
    })

    val innerPlan = newMockedLogicalPlan("b")

    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      strategy = newMockedStrategy(innerPlan),
      metrics = factory.newMetrics(hardcodedStatistics, mock[ExpressionEvaluator], config)
    )
    val left = newMockedLogicalPlanWithPatterns(Set(aNode))
    val plan = outerHashJoin(optionalQg, left).getOrElse(fail("No result from outerHashJoin"))

    plan should equal(OuterHashJoin(Set(aNode), left, innerPlan)(solved))
    plan.solved.lastQueryGraph.allHints should equal(theHint)
  }
}
