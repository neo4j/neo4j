/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps.argumentLeafPlanner
import org.neo4j.cypher.internal.ir.v3_5.{InterestingOrder, QueryGraph}
import org.neo4j.cypher.internal.v3_5.logical.plans.Argument
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class ArgumentLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should return an empty candidate list argument ids is empty") {
    val context = newMockedLogicalPlanningContext(newMockedPlanContext())

    val qg = QueryGraph(
      argumentIds = Set(),
      patternNodes = Set("a", "b")
    )

    argumentLeafPlanner(qg, InterestingOrder.empty, context) shouldBe empty
  }

  test("should return an empty candidate list pattern nodes is empty") {
    val context = newMockedLogicalPlanningContext(newMockedPlanContext())

    val qg = QueryGraph(
      argumentIds = Set("a", "b"),
      patternNodes = Set()
    )

    argumentLeafPlanner(qg, InterestingOrder.empty, context) shouldBe empty
  }

  test("should return a plan containing all the id in argument ids and in pattern nodes") {
    val context = newMockedLogicalPlanningContext(newMockedPlanContext())

    val qg = QueryGraph(
      argumentIds = Set("a", "b", "c"),
      patternNodes = Set("a", "b", "d")
    )

    argumentLeafPlanner(qg, InterestingOrder.empty, context) should equal(
      Seq(Argument(Set("a", "b","c")))
    )
  }
}
