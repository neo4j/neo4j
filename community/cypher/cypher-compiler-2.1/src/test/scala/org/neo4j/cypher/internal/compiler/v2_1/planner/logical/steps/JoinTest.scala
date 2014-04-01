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
import org.neo4j.cypher.InternalException

class JoinTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private def createQueryGraph(rels: PatternRelationship*) = QueryGraph(Map.empty, Selections(), Set.empty, rels.toSet)
  val aNode = IdName("a")
  val bNode = IdName("b")
  val cNode = IdName("c")
  val dNode = IdName("d")
  val r1Name = IdName("r1")
  val r2Name = IdName("r2")
  val r3Name = IdName("r3")
  val r1Rel = PatternRelationship(r1Name, (aNode, bNode), Direction.OUTGOING, Seq.empty)
  val r2Rel = PatternRelationship(r2Name, (bNode, cNode), Direction.OUTGOING, Seq.empty)
  val r3Rel = PatternRelationship(r3Name, (cNode, dNode), Direction.OUTGOING, Seq.empty)

  test("finds a single join") {
    implicit val context = newMockedLogicalPlanContext(queryGraph = createQueryGraph(r1Rel, r2Rel))
    val left: LogicalPlan = newMockedLogicalPlan(Set(aNode, bNode))
    val right: LogicalPlan = newMockedLogicalPlan(Set(bNode, cNode))
    val planTable = PlanTable(Map(
      Set(aNode, bNode) -> left,
      Set(bNode, cNode) -> right
    ))
    join(planTable) should equal(CandidateList(Seq(
      NodeHashJoin(IdName("b"), left, right),
      NodeHashJoin(IdName("b"), right, left)
    )))
  }

  test("finds multiple joins") {
    implicit val context = newMockedLogicalPlanContext(queryGraph = createQueryGraph(r1Rel, r2Rel, r3Rel))
    val left: LogicalPlan = newMockedLogicalPlan(Set(aNode, bNode))
    val middle: LogicalPlan = newMockedLogicalPlan(Set(bNode, cNode))
    val right: LogicalPlan = newMockedLogicalPlan(Set(cNode, dNode))
    val planTable = PlanTable(Map(
      Set(aNode, bNode) -> left,
      Set(bNode, cNode) -> middle,
      Set(cNode, dNode) -> right
    ))
    join(planTable) should equal(CandidateList(Seq(
      NodeHashJoin(IdName("b"), left, middle),
      NodeHashJoin(IdName("b"), middle, left),
      NodeHashJoin(IdName("c"), middle, right),
      NodeHashJoin(IdName("c"), right, middle)
    )))
  }

  test("throws an exception if there are plans with more than one overlapping ID") {
    implicit val context = newMockedLogicalPlanContext(queryGraph = createQueryGraph(r1Rel, r2Rel, r3Rel))
    val left: LogicalPlan = newMockedLogicalPlan(Set(aNode, bNode, cNode))
    val right: LogicalPlan = newMockedLogicalPlan(Set(bNode, cNode, dNode))
    val planTable = PlanTable(Map(
      Set(aNode, bNode, cNode) -> left,
      Set(bNode, cNode, dNode) -> right
    ))
    evaluating {
      join(planTable)
    } should produce[InternalException]
  }

  test("does not introduce joins if plans do not overlap") {
    implicit val context = newMockedLogicalPlanContext(queryGraph = createQueryGraph(r1Rel))
    val left: LogicalPlan = newMockedLogicalPlan(Set(aNode, bNode))
    val right: LogicalPlan = newMockedLogicalPlan(Set(cNode))
    val planTable = PlanTable(Map(
      Set(aNode, bNode) -> left,
      Set(cNode) -> right
    ))
    join(planTable) should equal(CandidateList())
  }

  test("does not join a plan with itself") {
    implicit val context = newMockedLogicalPlanContext(queryGraph = createQueryGraph())
    val left: LogicalPlan = newMockedLogicalPlan(Set(aNode))
    val planTable = PlanTable(Map(
      Set(aNode) -> left
    ))
    join(planTable) should equal(CandidateList())
  }
}
