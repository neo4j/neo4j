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

import org.neo4j.cypher.internal.compiler.v2_3.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class GreedyPlanTableTest extends CypherFunSuite with LogicalPlanningTestSupport {
  implicit val planContext = newMockedPlanContext
  implicit val context = newMockedLogicalPlanningContext(planContext)

  val x: LogicalPlan = newMockedLogicalPlan("x")
  val x2 = newMockedLogicalPlan("x")
  val y = newMockedLogicalPlan("y")
  val xAndY = newMockedLogicalPlan("x", "y")

  test("adding a new plan to an empty PlanTable returns a PlanTable with that plan in it") {
    val plans = GreedyPlanTable()
    val newPlans = plans + x

    newPlans should equal(GreedyPlanTable(x))
  }

  test("adding a new plan that does not cover anything else simply adds the plan") {
    val plans = GreedyPlanTable(x)
    val newPlans = plans + y

    newPlans should equal(GreedyPlanTable(x, y))
  }

  test("adding a plan that covers one other plan will remove that plan") {
    val plans = GreedyPlanTable(x)
    val newPlans = plans + xAndY

    newPlans should equal(GreedyPlanTable(xAndY))
  }

  test("adding a plan that is already covered by an existing plan results in no change") {
    val originalPlans = GreedyPlanTable(xAndY)
    val newPlans = originalPlans + x

    newPlans should equal(originalPlans)
  }

  test("adding a plan with the same ids covered does not replace it") {
    val originalPlans = GreedyPlanTable(x)
    val newPlans = originalPlans + x2

    newPlans should equal(originalPlans)
  }
}
