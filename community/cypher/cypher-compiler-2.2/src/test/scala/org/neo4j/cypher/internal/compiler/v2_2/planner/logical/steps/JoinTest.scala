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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.{QueryGraph, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Candidates, CandidateList, PlanTable}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.ast.PatternExpression

class JoinTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  private def createQuery(rels: PatternRelationship*) = QueryGraph.empty.addPatternRels(rels)

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

  test("finds a single join") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedLogicalPlan(Set(aNode, bNode))
    val right = newMockedLogicalPlan(Set(bNode, cNode))
    val planTable = PlanTable(Map(
      Set(aNode, bNode) -> left,
      Set(bNode, cNode) -> right
    ))

    val qg = createQuery(r1Rel, r2Rel)

    join(planTable, qg) should equal(Candidates(
      planNodeHashJoin(Set(bNode), left, right),
      planNodeHashJoin(Set(bNode), right, left)
    ))
  }

  test("finds a single join that overlaps two identifiers") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedLogicalPlan(Set(aNode, bNode, cNode))
    val right = newMockedLogicalPlan(Set(bNode, cNode, dNode))
    val planTable = PlanTable(Map(
      Set(aNode, bNode, cNode) -> left,
      Set(bNode, cNode, dNode) -> right
    ))

    val qg = createQuery(r1Rel, r2Rel)

    join(planTable, qg) should equal(Candidates(
      planNodeHashJoin(Set(bNode, cNode), left, right),
      planNodeHashJoin(Set(bNode, cNode), right, left)
    ))
  }

  test("finds multiple joins") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedLogicalPlanWithPatterns(Set(aNode, bNode))
    val middle = newMockedLogicalPlanWithPatterns(Set(bNode, cNode))
    val right = newMockedLogicalPlanWithPatterns(Set(cNode, dNode))
    val planTable = PlanTable(Map(
      Set(aNode, bNode) -> left,
      Set(bNode, cNode) -> middle,
      Set(cNode, dNode) -> right
    ))

    val qg = createQuery(r1Rel, r2Rel, r3Rel)

    join(planTable, qg) should equal(Candidates(
      planNodeHashJoin(Set(bNode), left, middle),
      planNodeHashJoin(Set(bNode), middle, left),
      planNodeHashJoin(Set(cNode), middle, right),
      planNodeHashJoin(Set(cNode), right, middle)
    ))
  }

  test("does not introduce joins if plans do not overlap") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedLogicalPlanWithPatterns(Set(aNode, bNode))
    val right = newMockedLogicalPlanWithPatterns(Set(cNode))
    val planTable = PlanTable(Map(
      Set(aNode, bNode) -> left,
      Set(cNode) -> right
    ))

    val qg = createQuery(r1Rel)

    join(planTable, qg) should equal(CandidateList())
  }

  test("does not join a plan with itself") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedLogicalPlanWithPatterns(Set(aNode))
    val planTable = PlanTable(Map(
      Set(aNode) -> left
    ))

    val qg = createQuery()

    join(planTable, qg) should equal(CandidateList())
  }

  test("does not join relationships") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedLogicalPlanWithPatterns(Set(r1Name, aNode))
    val right = newMockedLogicalPlanWithPatterns(Set(r1Name, bNode))
    val planTable = PlanTable(Map(
      Set(r1Name, aNode) -> left,
      Set(r1Name, bNode) -> right
    ))

    val qg = createQuery(r1Rel)
    join(planTable, qg) should equal(CandidateList())
  }
}
