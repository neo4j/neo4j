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
import org.neo4j.cypher.internal.compiler.v2_1.planner.{QueryGraph, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{Candidates, CandidateList, PlanTable}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Identifier, Equals}

class ExpandTest
  extends CypherFunSuite
  with LogicalPlanningTestSupport {

  private def createQueryGraph(rels: PatternRelationship*) = QueryGraph(patternRelationships = rels.toSet)
  val aNode = IdName("a")
  val bNode = IdName("b")
  val rName = IdName("r")
  val rRel = PatternRelationship(rName, (aNode, bNode), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val rVarRel = PatternRelationship(rName, (aNode, bNode), Direction.OUTGOING, Seq.empty, VarPatternLength.unlimited)
  val rSelfRel = PatternRelationship(rName, (aNode, aNode), Direction.OUTGOING, Seq.empty, SimplePatternLength)

  test("do not expand when no pattern relationships exist in query graph") {
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = createQueryGraph()
    )
    val plan = PlanTable(Map(Set(aNode) -> AllNodesScanPlan(aNode)))

    expand(plan) should equal(Candidates())
  }

  test("finds single pattern relationship when start point is picked") {
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = createQueryGraph(rRel)
    )
    val planA = newMockedQueryPlan("a")
    val plan = PlanTable(Map(Set(aNode) -> planA))

    expand(plan) should equal(Candidates(
      ExpandPlan(left = planA, from = aNode, Direction.OUTGOING, types = Seq.empty, to = bNode, rName, SimplePatternLength, rRel))
    )
  }

  test("finds expansion going both sides") {
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = createQueryGraph(rRel)
    )
    val planA = newMockedQueryPlan("a")
    val planB = newMockedQueryPlan("b")
    val plan = PlanTable(Map(Set(aNode) -> planA, Set(bNode) -> planB))

    expand(plan) should equal(CandidateList(Seq(
      ExpandPlan(left = planA, from = aNode, Direction.OUTGOING, types = Seq.empty, to = bNode, rName, SimplePatternLength, rRel),
      ExpandPlan(left = planB, from = bNode, Direction.INCOMING, types = Seq.empty, to = aNode, rName, SimplePatternLength, rRel)
    )))
  }

  test("does not include plan that has the relationship name already covered") {
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = createQueryGraph(rRel)
    )
    val aAndB = newMockedLogicalPlanWithPatterns(Set("a", "b"), Seq(rRel))
    val plan = PlanTable(Map(Set(aNode, bNode) -> aAndB))

    expand(plan) should equal(Candidates())
  }

  test("self referencing pattern is handled correctly") {
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = createQueryGraph(rSelfRel)
    )
    val planA = newMockedQueryPlan("a")
    val plan = PlanTable(Map(Set(aNode) -> planA))

    expand(plan) should equal(CandidateList(Seq(
      HiddenSelectionPlan(Seq(Equals(Identifier(aNode.name) _, Identifier(aNode.name + "$$$") _) _),
        ExpandPlan(left = planA, from = aNode, dir = Direction.OUTGOING, types = Seq.empty,
                   to = IdName(aNode.name + "$$$"), relName = rName, SimplePatternLength, rSelfRel)
      ))))
  }

  test("looping pattern is handled as it should") {
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = createQueryGraph(rRel)
    )
    val aAndB = newMockedQueryPlan("a", "b")
    val plan = PlanTable(Map(Set(aNode) -> aAndB))

    expand(plan) should equal(Candidates(
      HiddenSelectionPlan(Seq(Equals(Identifier(bNode.name)_, Identifier(bNode.name + "$$$")_)_),
        ExpandPlan(left = aAndB, from = aNode, dir = Direction.OUTGOING, types = Seq.empty,
          to = IdName(bNode.name + "$$$"), relName = rName, SimplePatternLength, mockRel)
      ),
      HiddenSelectionPlan(
        predicates = Seq(Equals(Identifier(aNode.name) _, Identifier(aNode.name + "$$$") _) _),
        left = ExpandPlan(left = aAndB, from = bNode, dir = Direction.INCOMING, types = Seq.empty,
          to = IdName(aNode.name + "$$$"), relName = rName, SimplePatternLength, mockRel)
      )))
  }

  test("unlimited variable length relationship") {
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = createQueryGraph(rVarRel)
    )
    val planA = newMockedQueryPlan("a")
    val plan = PlanTable(Map(Set(aNode) -> planA))

    expand(plan) should equal(Candidates(
      ExpandPlan(left = planA, from = aNode, dir = Direction.OUTGOING, types = Seq.empty, to = bNode, relName = rName, rVarRel.length, rVarRel)
    ))
  }
}
