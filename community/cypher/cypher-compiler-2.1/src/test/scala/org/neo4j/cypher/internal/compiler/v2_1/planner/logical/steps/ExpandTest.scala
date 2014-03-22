/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.{LogicalPlanningTestSupport, Selections, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{CandidateList, PlanTable}
import org.neo4j.graphdb.Direction

class ExpandTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private def createQueryGraph(rels: PatternRelationship*) = QueryGraph(Map.empty, Selections(), Set.empty, rels.toSet)
  val aNode = IdName("a")
  val bNode = IdName("b")
  val rName = IdName("r")
  val rRel = PatternRelationship(rName, (aNode, bNode), Direction.OUTGOING, Seq.empty)

  test("do not expand when no pattern relationships exist in querygraph") {
    implicit val context = newMockedLogicalPlanContext(queryGraph = createQueryGraph())
    val plan = PlanTable(Map(Set(aNode) -> AllNodesScan(aNode)))

    expand(plan) should equal(CandidateList())
  }

  test("finds single pattern relationship when start point is picked") {
    implicit val context = newMockedLogicalPlanContext(queryGraph = createQueryGraph(rRel))
    val planA = newMockedLogicalPlan("a")
    val plan = PlanTable(Map(Set(aNode) -> planA))

    expand(plan) should equal(CandidateList(Seq(
      Expand(left = planA, from = aNode, dir = Direction.OUTGOING, types = Seq.empty, to = bNode, relName = rName))))
  }

  test("finds expansion going both sides") {
    implicit val context = newMockedLogicalPlanContext(queryGraph = createQueryGraph(rRel))
    val planA = newMockedLogicalPlan("a")
    val planB = newMockedLogicalPlan("b")
    val plan = PlanTable(Map(Set(aNode) -> planA, Set(bNode) -> planB))

    expand(plan) should equal(CandidateList(Seq(
      Expand(left = planA, from = aNode, dir = Direction.OUTGOING, types = Seq.empty, to = bNode, relName = rName),
      Expand(left = planB, from = bNode, dir = Direction.INCOMING, types = Seq.empty, to = aNode, relName = rName)
    )))
  }

  test("does not include plan that has the relationship name already covered") {
    implicit val context = newMockedLogicalPlanContext(queryGraph = createQueryGraph(rRel))
    val aAndB = newMockedLogicalPlan("a", "b", "r")
    val plan = PlanTable(Map(Set(aNode, bNode) -> aAndB))

    expand(plan) should equal(CandidateList())
  }
}
