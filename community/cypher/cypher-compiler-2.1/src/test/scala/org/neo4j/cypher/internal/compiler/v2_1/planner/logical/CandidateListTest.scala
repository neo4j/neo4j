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
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{LogicalPlan, IdName}

class CandidateListTest extends CypherFunSuite with LogicalPlanningTestSupport {
  implicit val context = newMockedLogicalPlanContext()
  val x = newMockedLogicalPlan("x")
  val y = newMockedLogicalPlan("y")
  val xAndY = newMockedLogicalPlan("x", "y")

  test("prune with no overlaps returns the same candidates") {
    val candidates = CandidateList(Seq(x, y))
    candidates.pruned should equal(candidates)
  }

  test("prune with overlaps returns the first ones") {
    val candidates = CandidateList(Seq(x, xAndY))

    candidates.pruned should equal(CandidateList(Seq(x)))
  }

  test("empty prune is legal") {
    val candidates = CandidateList(Seq())

    candidates.pruned should equal(CandidateList(Seq()))
  }

  test("picks the right plan by cost, no matter the cardinality") {
    val a = newMockedLogicalPlan("a")
    val b = newMockedLogicalPlan("b")

    assertTopPlan(winner = b, a, b)(CostModel.lift {
      case `a` => 100
      case `b` => 50
    })
  }

  test("picks the right plan by cost, no matter the size of the covered ids") {
    val ab = newMockedLogicalPlan(Set(IdName("a"), IdName("b")))
    val b = newMockedLogicalPlan("b")

    assertTopPlan(winner = b, ab, b)(CostModel.lift {
      case `ab` => 100
      case `b` => 50
    })
  }

  test("picks the right plan by cost and secondly by the covered ids") {
    val ab = newMockedLogicalPlan(Set(IdName("a"), IdName("b")))
    val c = newMockedLogicalPlan("c")

    assertTopPlan(winner = ab, ab, c)(CostModel.lift {
      case `ab` => 50
      case `c` => 50
    })
  }

  private def assertTopPlan(winner: LogicalPlan, candidates: LogicalPlan*)(costs: CostModel) {
    CandidateList(candidates).topPlan(costs) should equal(Some(winner))
    CandidateList(candidates.reverse).topPlan(costs) should equal(Some(winner))
  }
}


