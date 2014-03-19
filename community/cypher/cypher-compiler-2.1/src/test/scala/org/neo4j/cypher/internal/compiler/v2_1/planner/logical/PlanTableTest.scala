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
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SimpleLogicalPlanner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport

class PlanTableTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val x = newMockedLogicalPlan("x")
  val y = newMockedLogicalPlan("y")
  val xAndY = newMockedLogicalPlan("x", "y")

  test("adding a new plan to an empty PlanTable returns a PlanTable with that plan in it") {
    val plans = PlanTable()
    val newPlans = plans ++ CandidateList(Seq(x))

    newPlans should equal(PlanTable(Map(x.asTableEntry)))
  }

  test("adding a new plan that does not cover anything else simply adds the plan") {
    val plans = PlanTable(Map(x.asTableEntry))
    val newPlans = plans ++ CandidateList(Seq(y))

    newPlans should equal(PlanTable(Map(x.asTableEntry, y.asTableEntry)))
  }

  test("adding a plan that covers one other plan will remove that plan") {
    val plans = PlanTable(Map(x.asTableEntry))
    val newPlans = plans ++ CandidateList(Seq(xAndY))

    newPlans should equal(PlanTable(Map(xAndY.asTableEntry)))
  }
}
