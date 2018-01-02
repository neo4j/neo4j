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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.greedy

import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ExpandTest
  extends CypherFunSuite
  with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  private def createQuery(rels: PatternRelationship*) = QueryGraph(patternRelationships = rels.toSet)
  val aNode = IdName("a")
  val bNode = IdName("b")
  val rName = IdName("r")
  val rRel = PatternRelationship(rName, (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  val rVarRel = PatternRelationship(rName, (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, VarPatternLength.unlimited)
  val rSelfRel = PatternRelationship(rName, (aNode, aNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  test("do not expand when no pattern relationships exist in query graph") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val plan = greedyPlanTableWith(AllNodesScan(aNode, Set.empty)(solved))

    val qg = createQuery()

    expand(plan, qg) shouldBe empty
  }

  test("finds single pattern relationship when start point is picked") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val planA = newMockedLogicalPlan("a")
    val plan = greedyPlanTableWith(planA)

    val qg = createQuery(rRel)

    expand(plan, qg) should equal(
      Seq(Expand(planA, aNode, SemanticDirection.OUTGOING, Seq.empty, bNode, rRel.name, ExpandAll)(solved))
    )
  }

  test("finds expansion going both sides") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val planA = newMockedLogicalPlan("a")
    val planB = newMockedLogicalPlan("b")
    val plan = greedyPlanTableWith(planA, planB)

    val qg = createQuery(rRel)

    expand(plan, qg).toList should equal(Seq(
      Expand(planA, aNode, SemanticDirection.OUTGOING, Seq.empty, bNode, rRel.name, mode = ExpandAll)(solved),
      Expand(planB, bNode, SemanticDirection.INCOMING, Seq.empty, aNode, rRel.name, mode = ExpandAll)(solved)
    ))
  }

  test("does not include plan that has the relationship name already covered") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val aAndB = newMockedLogicalPlanWithPatterns(Set("a", "b"), Seq(rRel))
    val plan = greedyPlanTableWith(aAndB)

    val qg = createQuery()

    expand(plan, qg) shouldBe empty
  }

  test("self referencing pattern is handled correctly") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val planA = newMockedLogicalPlan("a")
    val plan = greedyPlanTableWith(planA)

    val qg = createQuery(rSelfRel)

    expand(plan, qg) should equal(Seq(
      Expand(planA, aNode, SemanticDirection.OUTGOING, Seq.empty, aNode, rSelfRel.name, mode = ExpandInto)(solved)
    ))
  }

  test("looping pattern is handled as it should") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val aAndB = newMockedLogicalPlan("a", "b")
    val plan = greedyPlanTableWith(aAndB)

    val qg = createQuery(rRel)

    expand(plan, qg) should equal(Seq(
      Expand(aAndB, aNode, SemanticDirection.OUTGOING, Seq.empty, bNode, rRel.name, mode = ExpandInto)(solved),
      Expand(aAndB, bNode, SemanticDirection.INCOMING, Seq.empty, aNode, rRel.name, mode = ExpandInto)(solved)
      ))
  }

  test("unlimited variable length relationship") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val planA = newMockedLogicalPlan("a")
    val plan = greedyPlanTableWith(planA)

    val qg = createQuery(rVarRel)

    expand(plan, qg) should equal(
      Seq(VarExpand(planA, aNode, SemanticDirection.OUTGOING, rVarRel.dir, Seq.empty, bNode, rVarRel.name, mode = ExpandAll, length = VarPatternLength.unlimited)(solved))
    )
  }

  test("unlimited variable length relationship with a predicate on each relationship") {
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val planA = newMockedLogicalPlan("a")
    val plan = greedyPlanTableWith(planA)

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
      Seq(VarExpand(planA, aNode, SemanticDirection.OUTGOING, rVarRel.dir, Seq.empty, bNode, rVarRel.name, VarPatternLength.unlimited, ExpandAll, Seq(fooIdentifier -> innerPredicate))(solved))
    )
  }
}
