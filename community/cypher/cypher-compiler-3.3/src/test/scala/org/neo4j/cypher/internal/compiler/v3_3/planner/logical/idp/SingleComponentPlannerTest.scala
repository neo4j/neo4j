/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_4.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.ir.v3_4.{IdName, PatternRelationship, QueryGraph, SimplePatternLength}
import org.neo4j.cypher.internal.v3_4.logical.plans._

class SingleComponentPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport with AstConstructionTestSupport {
  test("plans expands for queries with single pattern rel") {
    // given
    val aNode = IdName("a")
    val bNode = IdName("b")
    val pattern = PatternRelationship(IdName("r1"), (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattern), patternNodes = Set(aNode, bNode))
    val aPlan = newMockedLogicalPlan("a")
    val bPlan = newMockedLogicalPlan("b")
    implicit val context = newMockedLogicalPlanningContext(planContext = mock[PlanContext])

    // when
    val logicalPlans = SingleComponentPlanner.planSinglePattern(qg, pattern, Set(aPlan, bPlan))

    // then
    val plan1 = Expand(aPlan, IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val plan2 = Expand(bPlan, IdName("b"), SemanticDirection.INCOMING, Seq.empty, IdName("a"), IdName("r1"), ExpandAll)(solved)
    val plan3 = Expand(CartesianProduct(aPlan, bPlan)(solved), IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandInto)(solved)
    assertPlansMatch(Set(plan1, plan2, plan3), logicalPlans.toSet)
  }

  test("plans hashjoins and cartesian product for queries with single pattern rel and multiple index hints") {
    // given
    val aNode = IdName("a")
    val bNode = IdName("b")
    val pattern = PatternRelationship(IdName("r1"), (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val hint1 = UsingIndexHint(varFor("a"), lblName("X"), Seq(PropertyKeyName("p")(pos)))(pos)
    val hint2 = UsingIndexHint(varFor("b"), lblName("X"), Seq(PropertyKeyName("p")(pos)))(pos)
    val qg = QueryGraph(patternRelationships = Set(pattern), patternNodes = Set(aNode, bNode), hints = Set(hint1, hint2))
    val aPlan = newMockedLogicalPlan("a")
    val bPlan = newMockedLogicalPlan("b")
    implicit val context = newMockedLogicalPlanningContext(planContext = mock[PlanContext])

    // when
    val logicalPlans = SingleComponentPlanner.planSinglePattern(qg, pattern, Set(aPlan, bPlan))

    // then

    val plan1 = Expand(aPlan, IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val plan2 = Expand(bPlan, IdName("b"), SemanticDirection.INCOMING, Seq.empty, IdName("a"), IdName("r1"), ExpandAll)(solved)
    val plan3a = NodeHashJoin(Set(bNode), plan1, bPlan)(solved)
    val plan3b = NodeHashJoin(Set(bNode), bPlan, plan1)(solved)
    val plan4a = NodeHashJoin(Set(aNode), plan2, aPlan)(solved)
    val plan4b = NodeHashJoin(Set(aNode), aPlan, plan2)(solved)
    val plan5 = Expand(CartesianProduct(aPlan, bPlan)(solved), IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandInto)(solved)
    assertPlansMatch(logicalPlans.toSet, Set(plan1, plan2, plan3a, plan3b, plan4a, plan4b, plan5))
  }

  test("plans hashjoins and cartesian product for queries with single pattern rel and a join hint") {
    // given
    val aNode = IdName("a")
    val bNode = IdName("b")
    val pattern = PatternRelationship(IdName("r1"), (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val hint = UsingJoinHint(Seq(varFor("a")))(pos)
    val qg = QueryGraph(patternRelationships = Set(pattern), patternNodes = Set(aNode, bNode), hints = Set(hint))
    val aPlan = newMockedLogicalPlan("a")
    val bPlan = newMockedLogicalPlan("b")
    implicit val context = newMockedLogicalPlanningContext(planContext = mock[PlanContext])

    // when
    val logicalPlans = SingleComponentPlanner.planSinglePattern(qg, pattern, Set(aPlan, bPlan))

    // then
    val plan1 = Expand(aPlan, IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val plan2 = Expand(bPlan, IdName("b"), SemanticDirection.INCOMING, Seq.empty, IdName("a"), IdName("r1"), ExpandAll)(solved)
    val plan3a = NodeHashJoin(Set(bNode), plan1, bPlan)(solved)
    val plan3b = NodeHashJoin(Set(bNode), bPlan, plan1)(solved)
    val plan4a = NodeHashJoin(Set(aNode), plan2, aPlan)(solved)
    val plan4b = NodeHashJoin(Set(aNode), aPlan, plan2)(solved)
    val plan5 = Expand(CartesianProduct(aPlan, bPlan)(solved), IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandInto)(solved)
    assertPlansMatch(logicalPlans.toSet, Set(plan1, plan2, plan3a, plan3b, plan4a, plan4b, plan5))

    assertPlanSolvesHints(logicalPlans.filter {
      case join: NodeHashJoin if join.nodes == Set(aNode) => true
      case _ => false
    }, hint)
  }

  test("plans hashjoins and cartesian product for queries with single pattern rel and a join hint on the end node") {
    // given
    val aNode = IdName("a")
    val bNode = IdName("b")
    val pattern = PatternRelationship(IdName("r1"), (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val hint = UsingJoinHint(Seq(varFor("b")))(pos)
    val qg = QueryGraph(patternRelationships = Set(pattern), patternNodes = Set(aNode, bNode), hints = Set(hint))
    val aPlan = newMockedLogicalPlan("a")
    val bPlan = newMockedLogicalPlan("b")
    implicit val context = newMockedLogicalPlanningContext(planContext = mock[PlanContext])

    // when
    val logicalPlans = SingleComponentPlanner.planSinglePattern(qg, pattern, Set(aPlan, bPlan))

    // then
    val plan1 = Expand(aPlan, IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val plan2 = Expand(bPlan, IdName("b"), SemanticDirection.INCOMING, Seq.empty, IdName("a"), IdName("r1"), ExpandAll)(solved)
    val plan3a = NodeHashJoin(Set(bNode), plan1, bPlan)(solved)
    val plan3b = NodeHashJoin(Set(bNode), bPlan, plan1)(solved)
    val plan4a = NodeHashJoin(Set(aNode), plan2, aPlan)(solved)
    val plan4b = NodeHashJoin(Set(aNode), aPlan, plan2)(solved)
    val plan5 = Expand(CartesianProduct(aPlan, bPlan)(solved), IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandInto)(solved)
    assertPlansMatch(logicalPlans.toSet, Set(plan1, plan2, plan3a, plan3b, plan4a, plan4b, plan5))

    assertPlanSolvesHints(logicalPlans.filter {
      case join: NodeHashJoin if join.nodes == Set(bNode) => true
      case _ => false
    }, hint)
  }

  private def assertPlansMatch(expected: Set[LogicalPlan], actualPlans: Set[LogicalPlan]) {
    actualPlans.foreach(actual => expected should contain(actual))
    actualPlans.size should be(expected.size)
  }

  private def assertPlanSolvesHints(plans: Iterable[LogicalPlan], hints: Hint*) {
    for (h <- hints;
         p <- plans) {
      p.solved.lastQueryGraph.hints should contain(h)
    }
  }
}
