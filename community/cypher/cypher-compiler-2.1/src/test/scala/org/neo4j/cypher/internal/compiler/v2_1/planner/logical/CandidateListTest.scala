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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.{PlannerQuery, LogicalPlanningTestSupport2}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.QueryPlan
import org.neo4j.cypher.internal.compiler.v2_1.ast.{LabelName, UsingIndexHint}

class CandidateListTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  val GIVEN_FIXED_COST = new given {
    cost = {
      case _ => Cost(100)
    }
  }

  val hint1: UsingIndexHint = UsingIndexHint(ident("n"), LabelName("Person")_, ident("name"))_
  val hint2: UsingIndexHint = UsingIndexHint(ident("n"), LabelName("Person")_, ident("age"))_
  val hint3: UsingIndexHint = UsingIndexHint(ident("n"), LabelName("Person")_, ident("income"))_

  test("picks the right plan by cost, no matter the cardinality") {
    val a = fakeQueryPlanFor("a")
    val b = fakeQueryPlanFor("b")

    assertTopPlan(winner = b, a, b)(new given {
      cost = {
        case p if p == a.plan => Cost(100)
        case p if p == b.plan => Cost(50)
      }
    })
  }

  test("picks the right plan by cost, no matter the size of the covered ids") {
    val ab = fakeQueryPlanFor("a", "b")
    val b = fakeQueryPlanFor("b")

    val GIVEN = new given {
      cost = {
        case p if p == ab.plan => Cost(100)
        case p if p == b.plan => Cost(50)
      }
    }

    assertTopPlan(winner = b, ab, b)(GIVEN)
  }

  test("picks the right plan by cost and secondly by the covered ids") {
    val ab = fakeQueryPlanFor("a", "b")
    val c = fakeQueryPlanFor("c")

    assertTopPlan(winner = ab, ab, c)(GIVEN_FIXED_COST)
  }

  test("Prefers plans that solves a hint over plan that solves no hint") {
    val a = fakeQueryPlanFor("a").updateSolved(_.updateGraph(_.addHints(Some(hint1))))
    val b = fakeQueryPlanFor("a")

    assertTopPlan(winner = a, a, b)(GIVEN_FIXED_COST)
  }

  test("Prefers plans that solve more hints") {
    val a = fakeQueryPlanFor("a").updateSolved(_.updateGraph(_.addHints(Some(hint1))))
    val b = fakeQueryPlanFor("a").updateSolved(_.updateGraph(_.addHints(Seq(hint1, hint2))))

    assertTopPlan(winner = b, a, b)(GIVEN_FIXED_COST)
  }

  test("Prefers plans that solve more hints in tails") {
    val a = fakeQueryPlanFor("a").updateSolved(_.updateGraph(_.addHints(Some(hint1))))
    val b = fakeQueryPlanFor("a").updateSolved(_.withTail(PlannerQuery.empty.updateGraph(_.addHints(Seq(hint1, hint2)))))

    assertTopPlan(winner = b, a, b)(GIVEN_FIXED_COST)
  }

  private def assertTopPlan(winner: QueryPlan, candidates: QueryPlan*)(GIVEN: given) {
    val environment = LogicalPlanningEnvironment(GIVEN)
    val costs = environment.metricsFactory.newMetrics(GIVEN.graphStatistics, environment.semanticTable).cost
    CandidateList(candidates).bestPlan(costs) should equal(Some(winner))
    CandidateList(candidates.reverse).bestPlan(costs) should equal(Some(winner))
  }
}


