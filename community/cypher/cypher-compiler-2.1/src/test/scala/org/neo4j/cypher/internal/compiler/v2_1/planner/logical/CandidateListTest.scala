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

package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.QueryPlan

class CandidateListTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  test("picks the right plan by cost, no matter the cardinality") {
    val a = fakeQueryPlanFor("a")
    val b = fakeQueryPlanFor("b")

    assertTopPlan(winner = b, a, b)(new given {
      cost = {
        case p if p == a.plan => 100
        case p if p == b.plan => 50
      }
    })
  }


  test("picks the right plan by cost, no matter the size of the covered ids") {
    val ab = fakeQueryPlanFor("a", "b")
    val b = fakeQueryPlanFor("b")

    val GIVEN = new given {
      cost = {
        case p if p == ab.plan => 100
        case p if p == b.plan => 50
      }
    }

    assertTopPlan(winner = b, ab, b)(GIVEN)
  }

  test("picks the right plan by cost and secondly by the covered ids") {
    val ab = fakeQueryPlanFor("a", "b")
    val c = fakeQueryPlanFor("c")

    val GIVEN = new given {
      cost = {
        case _ => 100
      }
    }

    assertTopPlan(winner = ab, ab, c)(GIVEN)
  }

  private def assertTopPlan(winner: QueryPlan, candidates: QueryPlan*)(GIVEN: given) {
    val environment = LogicalPlanningEnvironment(GIVEN)
    val costs = environment.metricsFactory.newMetrics(GIVEN.graphStatistics, environment.semanticTable).cost
    CandidateList(candidates).bestPlan(costs) should equal(Some(winner))
    CandidateList(candidates.reverse).bestPlan(costs) should equal(Some(winner))
  }
}


