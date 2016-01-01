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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.{QueryGraph, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{Candidates, CandidateList, PlanTable}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_1.ast.PatternExpression

class JoinTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  private def createQuery(rels: PatternRelationship*) = QueryGraph(patternRelationships = rels.toSet)
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
    val left = newMockedQueryPlan(Set(aNode, bNode))
    val right = newMockedQueryPlan(Set(bNode, cNode))
    val planTable = PlanTable(Map(
      Set(aNode, bNode) -> left,
      Set(bNode, cNode) -> right
    ))

    val qg = createQuery(r1Rel, r2Rel)

    join(planTable, qg) should equal(Candidates(
      planNodeHashJoin(IdName("b"), left, right),
      planNodeHashJoin(IdName("b"), right, left)
    ))
  }

  test("finds multiple joins") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedQueryPlanWithPatterns(Set(aNode, bNode))
    val middle = newMockedQueryPlanWithPatterns(Set(bNode, cNode))
    val right = newMockedQueryPlanWithPatterns(Set(cNode, dNode))
    val planTable = PlanTable(Map(
      Set(aNode, bNode) -> left,
      Set(bNode, cNode) -> middle,
      Set(cNode, dNode) -> right
    ))

    val qg = createQuery(r1Rel, r2Rel, r3Rel)

    join(planTable, qg) should equal(Candidates(
      planNodeHashJoin(IdName("b"), left, middle),
      planNodeHashJoin(IdName("b"), middle, left),
      planNodeHashJoin(IdName("c"), middle, right),
      planNodeHashJoin(IdName("c"), right, middle)
    ))
  }

  test("does not introduce joins if plans do not overlap") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val left = newMockedQueryPlanWithPatterns(Set(aNode, bNode))
    val right = newMockedQueryPlanWithPatterns(Set(cNode))
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
    val left = newMockedQueryPlanWithPatterns(Set(aNode))
    val planTable = PlanTable(Map(
      Set(aNode) -> left
    ))

    val qg = createQuery()

    join(planTable, qg) should equal(CandidateList())
  }
}
