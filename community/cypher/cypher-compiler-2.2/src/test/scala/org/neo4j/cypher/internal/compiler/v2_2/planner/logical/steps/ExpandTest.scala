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
import org.neo4j.cypher.internal.compiler.v2_2.planner.{Predicate, Selections, QueryGraph, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Candidates, CandidateList, PlanTable}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._

class ExpandTest
  extends CypherFunSuite
  with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  private def createQuery(rels: PatternRelationship*) = QueryGraph(patternRelationships = rels.toSet)
  val aNode = IdName("a")
  val bNode = IdName("b")
  val rName = IdName("r")
  val rRel = PatternRelationship(rName, (aNode, bNode), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val rVarRel = PatternRelationship(rName, (aNode, bNode), Direction.OUTGOING, Seq.empty, VarPatternLength.unlimited)
  val rSelfRel = PatternRelationship(rName, (aNode, aNode), Direction.OUTGOING, Seq.empty, SimplePatternLength)

  test("do not expand when no pattern relationships exist in query graph") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val plan = PlanTable(Map(Set(aNode) -> planAllNodesScan(aNode, Set.empty)))

    val qg = createQuery()

    expand(plan, qg) should equal(Candidates())
  }

  test("finds single pattern relationship when start point is picked") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val planA = newMockedLogicalPlan("a")
    val plan = PlanTable(Map(Set(aNode) -> planA))

    val qg = createQuery(rRel)

    expand(plan, qg) should equal(Candidates(
      planExpand(left = planA, from = aNode, Direction.OUTGOING, Direction.OUTGOING, types = Seq.empty, to = bNode, rName, SimplePatternLength, rRel, Seq.empty, Seq.empty))
    )
  }

  test("finds expansion going both sides") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val planA = newMockedLogicalPlan("a")
    val planB = newMockedLogicalPlan("b")
    val plan = PlanTable(Map(Set(aNode) -> planA, Set(bNode) -> planB))

    val qg = createQuery(rRel)

    expand(plan, qg) should equal(CandidateList(Seq(
      planExpand(left = planA, from = aNode, Direction.OUTGOING, Direction.OUTGOING, types = Seq.empty, to = bNode, rName, SimplePatternLength, rRel, Seq.empty, Seq.empty),
      planExpand(left = planB, from = bNode, Direction.INCOMING, Direction.OUTGOING, types = Seq.empty, to = aNode, rName, SimplePatternLength, rRel, Seq.empty, Seq.empty)
    )))
  }

  test("does not include plan that has the relationship name already covered") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val aAndB = newMockedLogicalPlanWithPatterns(Set("a", "b"), Seq(rRel))
    val plan = PlanTable(Map(Set(aNode, bNode) -> aAndB))

    val qg = createQuery()

    expand(plan, qg) should equal(Candidates())
  }

  test("self referencing pattern is handled correctly") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val planA = newMockedLogicalPlan("a")
    val plan = PlanTable(Map(Set(aNode) -> planA))

    val qg = createQuery(rSelfRel)

    expand(plan, qg) should equal(CandidateList(Seq(
      planHiddenSelection(Seq(Equals(Identifier(aNode.name) _, Identifier(aNode.name + "$$$") _) _),
        planExpand(left = planA, from = aNode, dir = Direction.OUTGOING, Direction.OUTGOING, types = Seq.empty,
                   to = IdName(aNode.name + "$$$"), relName = rName, SimplePatternLength, rSelfRel, Seq.empty, Seq.empty)
      ))))
  }

  test("looping pattern is handled as it should") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val aAndB = newMockedLogicalPlan("a", "b")
    val plan = PlanTable(Map(Set(aNode) -> aAndB))

    val qg = createQuery(rRel)

    expand(plan, qg) should equal(Candidates(
      planHiddenSelection(Seq(Equals(Identifier(bNode.name)_, Identifier(bNode.name + "$$$")_)_),
        planExpand(left = aAndB, from = aNode, dir = Direction.OUTGOING, Direction.OUTGOING, types = Seq.empty,
          to = IdName(bNode.name + "$$$"), relName = rName, SimplePatternLength, mockRel, Seq.empty, Seq.empty)
      ),
      planHiddenSelection(
        predicates = Seq(Equals(Identifier(aNode.name) _, Identifier(aNode.name + "$$$") _) _),
        left = planExpand(left = aAndB, from = bNode, dir = Direction.INCOMING, Direction.OUTGOING, types = Seq.empty,
          to = IdName(aNode.name + "$$$"), relName = rName, SimplePatternLength, mockRel, Seq.empty, Seq.empty)
      )))
  }

  test("unlimited variable length relationship") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val planA = newMockedLogicalPlan("a")
    val plan = PlanTable(Map(Set(aNode) -> planA))

    val qg = createQuery(rVarRel)

    expand(plan, qg) should equal(Candidates(
      planExpand(left = planA, from = aNode, dir = Direction.OUTGOING, Direction.OUTGOING, types = Seq.empty, to = bNode, relName = rName, rVarRel.length, rVarRel, Seq.empty, Seq.empty)
    ))
  }

  test("unlimited variable length relationship with a predicate on each relationship") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val planA = newMockedLogicalPlan("a")
    val plan = PlanTable(Map(Set(aNode) -> planA))

    val relIdentifier: Identifier = Identifier(rName.name)_
    val innerPredicate: Expression = Equals(Property(Identifier("foo")_, PropertyKeyName("prop")_)_, SignedDecimalIntegerLiteral("20")_)_
    val allPredicate = AllIterablePredicate(
      Identifier("foo")_,
      relIdentifier,
      Some(innerPredicate) // foo.prop = 20
    )_
    val predicate: Predicate = Predicate(Set(rName), allPredicate)
    val qg = createQuery(rVarRel).addSelections(Selections(Set(predicate)))
    val fooIdentifier: Identifier = Identifier("foo")_
    val result = expand(plan, qg)
    result should equal(Candidates(
      planExpand(left = planA, from = aNode, dir = Direction.OUTGOING, Direction.OUTGOING, types = Seq.empty,
                 to = bNode, relName = rName, rVarRel.length, rVarRel, Seq(fooIdentifier -> innerPredicate), Seq(allPredicate))
    ))
  }
}
