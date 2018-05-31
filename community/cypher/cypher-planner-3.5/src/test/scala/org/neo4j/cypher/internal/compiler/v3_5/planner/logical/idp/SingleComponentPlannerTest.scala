/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.idp

import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport
import org.opencypher.v9_0.ast._
import org.neo4j.cypher.internal.ir.v3_5.{PatternRelationship, QueryGraph, SimplePatternLength}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.expressions.{PropertyKeyName, SemanticDirection}

class SingleComponentPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport with AstConstructionTestSupport {
  test("plans expands for queries with single pattern rel") {
    // given
    val aNode = "a"
    val bNode = "b"
    val pattern = PatternRelationship("r1", (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattern), patternNodes = Set(aNode, bNode))
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(planContext = mock[PlanContext])
    val aPlan = newMockedLogicalPlan(solveds, cardinalities,  "a")
    val bPlan = newMockedLogicalPlan(solveds, cardinalities, "b")

    // when
    val logicalPlans = SingleComponentPlanner.planSinglePattern(qg, pattern, Set(aPlan, bPlan), context, solveds)

    // then
    val plan1 = Expand(aPlan, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandAll)
    val plan2 = Expand(bPlan, "b", SemanticDirection.INCOMING, Seq.empty, "a", "r1", ExpandAll)
    val plan3 = Expand(CartesianProduct(aPlan, bPlan), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandInto)
    assertPlansMatch(Set(plan1, plan2, plan3), logicalPlans.toSet)
  }

  test("plans hashjoins and cartesian product for queries with single pattern rel and multiple index hints") {
    // given
    val aNode = "a"
    val bNode = "b"
    val pattern = PatternRelationship("r1", (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val hint1 = UsingIndexHint(varFor("a"), lblName("X"), Seq(PropertyKeyName("p")(pos)))(pos)
    val hint2 = UsingIndexHint(varFor("b"), lblName("X"), Seq(PropertyKeyName("p")(pos)))(pos)
    val qg = QueryGraph(patternRelationships = Set(pattern), patternNodes = Set(aNode, bNode), hints = Seq(hint1, hint2))
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(planContext = mock[PlanContext])
    val aPlan = newMockedLogicalPlan(solveds, cardinalities, "a")
    val bPlan = newMockedLogicalPlan(solveds, cardinalities, "b")

    // when
    val logicalPlans = SingleComponentPlanner.planSinglePattern(qg, pattern, Set(aPlan, bPlan), context, solveds)

    // then

    val plan1 = Expand(aPlan, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandAll)
    val plan2 = Expand(bPlan, "b", SemanticDirection.INCOMING, Seq.empty, "a", "r1", ExpandAll)
    val plan3a = NodeHashJoin(Set(bNode), plan1, bPlan)
    val plan3b = NodeHashJoin(Set(bNode), bPlan, plan1)
    val plan4a = NodeHashJoin(Set(aNode), plan2, aPlan)
    val plan4b = NodeHashJoin(Set(aNode), aPlan, plan2)
    val plan5 = Expand(CartesianProduct(aPlan, bPlan), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandInto)
    assertPlansMatch(logicalPlans.toSet, Set(plan1, plan2, plan3a, plan3b, plan4a, plan4b, plan5))
  }

  test("plans hashjoins and cartesian product for queries with single pattern rel and a join hint") {
    // given
    val aNode = "a"
    val bNode = "b"
    val pattern = PatternRelationship("r1", (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val hint = UsingJoinHint(Seq(varFor("a")))(pos)
    val qg = QueryGraph(patternRelationships = Set(pattern), patternNodes = Set(aNode, bNode), hints = Seq(hint))
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(planContext = mock[PlanContext])
    val aPlan = newMockedLogicalPlan(solveds, cardinalities, "a")
    val bPlan = newMockedLogicalPlan(solveds, cardinalities, "b")

    // when
    val logicalPlans = SingleComponentPlanner.planSinglePattern(qg, pattern, Set(aPlan, bPlan), context, solveds)

    // then
    val plan1 = Expand(aPlan, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandAll)
    val plan2 = Expand(bPlan, "b", SemanticDirection.INCOMING, Seq.empty, "a", "r1", ExpandAll)
    val plan3a = NodeHashJoin(Set(bNode), plan1, bPlan)
    val plan3b = NodeHashJoin(Set(bNode), bPlan, plan1)
    val plan4a = NodeHashJoin(Set(aNode), plan2, aPlan)
    val plan4b = NodeHashJoin(Set(aNode), aPlan, plan2)
    val plan5 = Expand(CartesianProduct(aPlan, bPlan), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandInto)
    assertPlansMatch(logicalPlans.toSet, Set(plan1, plan2, plan3a, plan3b, plan4a, plan4b, plan5))

    assertPlanSolvesHints(logicalPlans.filter {
      case join: NodeHashJoin if join.nodes == Set(aNode) => true
      case _ => false
    }, solveds, hint)
  }

  test("plans hashjoins and cartesian product for queries with single pattern rel and a join hint on the end node") {
    // given
    val aNode = "a"
    val bNode = "b"
    val pattern = PatternRelationship("r1", (aNode, bNode), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val hint = UsingJoinHint(Seq(varFor("b")))(pos)
    val qg = QueryGraph(patternRelationships = Set(pattern), patternNodes = Set(aNode, bNode), hints = Seq(hint))
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(planContext = mock[PlanContext])
    val aPlan = newMockedLogicalPlan(solveds, cardinalities, "a")
    val bPlan = newMockedLogicalPlan(solveds, cardinalities, "b")

    // when
    val logicalPlans = SingleComponentPlanner.planSinglePattern(qg, pattern, Set(aPlan, bPlan), context, solveds)

    // then
    val plan1 = Expand(aPlan, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandAll)
    val plan2 = Expand(bPlan, "b", SemanticDirection.INCOMING, Seq.empty, "a", "r1", ExpandAll)
    val plan3a = NodeHashJoin(Set(bNode), plan1, bPlan)
    val plan3b = NodeHashJoin(Set(bNode), bPlan, plan1)
    val plan4a = NodeHashJoin(Set(aNode), plan2, aPlan)
    val plan4b = NodeHashJoin(Set(aNode), aPlan, plan2)
    val plan5 = Expand(CartesianProduct(aPlan, bPlan), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandInto)
    assertPlansMatch(logicalPlans.toSet, Set(plan1, plan2, plan3a, plan3b, plan4a, plan4b, plan5))

    assertPlanSolvesHints(logicalPlans.filter {
      case join: NodeHashJoin if join.nodes == Set(bNode) => true
      case _ => false
    }, solveds, hint)
  }

  private def assertPlansMatch(expected: Set[LogicalPlan], actualPlans: Set[LogicalPlan]) {
    actualPlans.foreach(actual => expected should contain(actual))
    actualPlans.size should be(expected.size)
  }

  private def assertPlanSolvesHints(plans: Iterable[LogicalPlan], solveds: Solveds, hints: Hint*) {
    for (h <- hints;
         p <- plans) {
      solveds.get(p.id).lastQueryGraph.hints should contain(h)
    }
  }
}
