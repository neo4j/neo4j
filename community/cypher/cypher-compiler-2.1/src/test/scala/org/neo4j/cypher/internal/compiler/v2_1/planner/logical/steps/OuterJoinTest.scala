/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{Cardinality, CandidateList, PlanTable}
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_1.ast.PatternExpression

class OuterJoinTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  val aNode = IdName("a")
  val bNode = IdName("b")
  val cNode = IdName("c")
  val dNode = IdName("d")
  val r1Name = IdName("r1")
  val r2Name = IdName("r2")
  val r3Name = IdName("r3")
  val r1Rel = PatternRelationship(r1Name, (aNode, bNode), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val r2Rel = PatternRelationship(r2Name, (bNode, cNode), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val r3Rel = PatternRelationship(r3Name, (cNode, dNode), Direction.OUTGOING, Seq.empty, SimplePatternLength)

  test("does not try to join anything if optional pattern is not present") {
    // MATCH (a)-->(b)
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedQueryPlan(Set(aNode, bNode))
    val planTable = PlanTable(Map(Set(aNode, bNode) -> left))

    val qg = QueryGraph(
        patternNodes = Set(aNode, bNode),
        patternRelationships = Set(r1Rel)
      )

    outerJoin(planTable, qg) should equal(CandidateList(Seq.empty))
  }

  test("solve optional match with outer join") {
    // MATCH a OPTIONAL MATCH a-->b
    val optionalQg = QueryGraph(
      patternNodes = Set(aNode, bNode),
      patternRelationships = Set(r1Rel))

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case AllNodesScan(IdName("b")) => Cardinality(1) // Make sure we start the inner plan using b
      case _                         => Cardinality(1000)
    })

    val innerPlan = newMockedQueryPlan("b")

    val qg = QueryGraph(patternNodes = Set(aNode)).withAddedOptionalMatch(optionalQg)

    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      strategy = newMockedStrategy(innerPlan),
      metrics = factory.newMetrics(hardcodedStatistics, newMockedSemanticTable)
    )
    val left = newMockedQueryPlanWithPatterns(Set(aNode))
    val planTable = PlanTable(Map(Set(aNode) -> left))

    val expectedPlan = planOuterHashJoin(aNode, left, innerPlan)

    outerJoin(planTable, qg).plans.head should equal(expectedPlan)
  }
}
