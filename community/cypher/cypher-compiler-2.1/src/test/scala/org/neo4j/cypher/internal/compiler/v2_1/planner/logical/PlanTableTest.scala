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
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport

class PlanTableTest extends CypherFunSuite with LogicalPlanningTestSupport {
  implicit val planContext = newMockedPlanContext
  implicit val context = newMockedLogicalPlanningContext(planContext)

  val x = newMockedQueryPlan("x")
  val x2 = newMockedQueryPlan("x")
  val y = newMockedQueryPlan("y")
  val xAndY = newMockedQueryPlan("x", "y")

  test("adding a new plan to an empty PlanTable returns a PlanTable with that plan in it") {
    val plans = PlanTable()
    val newPlans = plans + x

    newPlans should equal(PlanTable(Map(x.asTableEntry)))
  }

  test("adding a new plan that does not cover anything else simply adds the plan") {
    val plans = PlanTable(Map(x.asTableEntry))
    val newPlans = plans + y

    newPlans should equal(PlanTable(Map(x.asTableEntry, y.asTableEntry)))
  }

  test("adding a plan that covers one other plan will remove that plan") {
    val plans = PlanTable(Map(x.asTableEntry))
    val newPlans = plans + xAndY

    newPlans should equal(PlanTable(Map(xAndY.asTableEntry)))
  }

  test("adding a plan that is already covered by an existing plan results in no change") {
    val originalPlans = PlanTable(Map(xAndY.asTableEntry))
    val newPlans = originalPlans + x

    newPlans should equal(originalPlans)
  }

  test("adding a plan with the same ids covered does not replace it") {
    val originalPlans = PlanTable(Map(x.asTableEntry))
    val newPlans = originalPlans + x2

    newPlans should equal(originalPlans)
  }
}
