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
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SimpleLogicalPlanner._

class PlanTableTest extends CypherFunSuite {

  val x = plan("x")
  val xIds = Set(IdName("x"))

  val y = plan("y")
  val yIds = Set(IdName("y"))

  val xAndY = plan("x", "y")
  val xAndYIds = Set(IdName("x"), IdName("y"))

  test("adding a new plan to an empty PlanTable returns a PlanTable with that plan in it") {
    val plans = PlanTable()
    val newPlan = plans ++ CandidateList(Seq(x))

    val expected = PlanTable(Map(xIds -> x))

    newPlan should equal(expected)
  }

  test("adding a new plan that does not cover anything else simply adds the plan") {
    val plans = PlanTable(Map(xIds -> x))
    val newPlan = plans ++ CandidateList(Seq(y))

    val expected = PlanTable(Map(xIds -> x, yIds -> y))

    newPlan should equal(expected)
  }

  test("adding a plan that covers one other plan will remove that plan") {
    val plans = PlanTable(Map(xIds -> x))
    val newPlan = plans ++ CandidateList(Seq(xAndY))

    val expected = PlanTable(Map(xAndYIds -> xAndY))

    newPlan should equal(expected)
  }

  def plan(ids: String*): PlanTableEntry = {
    val plan = mock[LogicalPlan]
    when(plan.toString).thenReturn(ids.mkString)
    PlanTableEntry(plan, Seq.empty, 0, ids.map(IdName.apply).toSet, 0)
  }
}
