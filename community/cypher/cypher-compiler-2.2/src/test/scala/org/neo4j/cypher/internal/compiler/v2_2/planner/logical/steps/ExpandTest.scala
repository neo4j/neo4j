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
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.PlanTable
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport, Predicate, QueryGraph, Selections}
import org.neo4j.graphdb.Direction

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

    expand(plan, qg) shouldBe empty
  }

  test("finds single pattern relationship when start point is picked") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val planA = newMockedLogicalPlan("a")
    val plan = PlanTable(Map(Set(aNode) -> planA))

    val qg = createQuery(rRel)

    expand(plan, qg) should equal(
      Seq(planSimpleExpand(planA, aNode, Direction.OUTGOING, bNode, rRel, ExpandAll))
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

    expand(plan, qg) should equal(Seq(
      planSimpleExpand(left = planA, from = aNode, Direction.OUTGOING, to = bNode, pattern = rRel, mode = ExpandAll),
      planSimpleExpand(left = planB, from = bNode, Direction.INCOMING, to = aNode, pattern = rRel, mode = ExpandAll)
    ))
  }

  test("does not include plan that has the relationship name already covered") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val aAndB = newMockedLogicalPlanWithPatterns(Set("a", "b"), Seq(rRel))
    val plan = PlanTable(Map(Set(aNode, bNode) -> aAndB))

    val qg = createQuery()

    expand(plan, qg) shouldBe empty
  }

  test("self referencing pattern is handled correctly") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val planA = newMockedLogicalPlan("a")
    val plan = PlanTable(Map(Set(aNode) -> planA))

    val qg = createQuery(rSelfRel)

    expand(plan, qg) should equal(Seq(
      planSimpleExpand(left = planA, from = aNode, dir = Direction.OUTGOING, to = IdName(aNode.name), pattern = rSelfRel, mode = ExpandInto)
    ))
  }

  test("looping pattern is handled as it should") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val aAndB = newMockedLogicalPlan("a", "b")
    val plan = PlanTable(Map(Set(aNode) -> aAndB))

    val qg = createQuery(rRel)

    expand(plan, qg) should equal(Seq(
      planSimpleExpand(left = aAndB, from = aNode, dir = Direction.OUTGOING, to = IdName(bNode.name), pattern =mockRel, mode = ExpandInto),
      planSimpleExpand(left = aAndB, from = bNode, dir = Direction.INCOMING, to = IdName(aNode.name), pattern =mockRel, mode = ExpandInto)
      ))
  }

  test("unlimited variable length relationship") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val planA = newMockedLogicalPlan("a")
    val plan = PlanTable(Map(Set(aNode) -> planA))

    val qg = createQuery(rVarRel)

    expand(plan, qg) should equal(
      Seq(planVarExpand(left = planA, from = aNode, dir = Direction.OUTGOING, to = bNode, pattern = rVarRel, mode = ExpandAll, predicates = Seq.empty, allPredicates = Seq.empty))
    )
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
    result should equal(
      Seq(planVarExpand(left = planA, from = aNode, dir = Direction.OUTGOING, to = bNode, pattern = rVarRel,
        predicates = Seq(fooIdentifier -> innerPredicate), allPredicates = Seq(allPredicate), mode = ExpandAll))
    )
  }
}
