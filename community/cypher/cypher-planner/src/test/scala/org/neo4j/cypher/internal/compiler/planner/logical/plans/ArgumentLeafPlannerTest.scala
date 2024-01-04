/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.argumentLeafPlanner
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ArgumentLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should return an empty candidate list argument ids is empty") {
    val context = newMockedLogicalPlanningContext(newMockedPlanContext())

    val qg = QueryGraph(
      argumentIds = Set(),
      patternNodes = Set(v"a", v"b")
    )

    argumentLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context) shouldBe empty
  }

  test("should return an empty candidate list pattern nodes is empty") {
    val context = newMockedLogicalPlanningContext(newMockedPlanContext())

    val qg = QueryGraph(
      argumentIds = Set(v"a", v"b"),
      patternNodes = Set()
    )

    argumentLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context) shouldBe empty
  }

  test("should return a plan containing all the id in argument ids and in pattern nodes") {
    val context = newMockedLogicalPlanningContext(newMockedPlanContext())

    val qg = QueryGraph(
      argumentIds = Set(v"a", v"b", v"c"),
      patternNodes = Set(v"a", v"b", v"d")
    )

    argumentLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context) should equal(
      Set(Argument(Set(v"a", v"b", v"c")))
    )
  }

  test("should not plan argument for skipped id") {
    // given
    val context = newMockedLogicalPlanningContext(newMockedPlanContext())
    val queryGraph = QueryGraph(
      argumentIds = Set(v"n"),
      patternNodes = Set(v"n")
    )

    // when
    val resultPlans = argumentLeafPlanner(Set(v"n"))(queryGraph, InterestingOrderConfig.empty, context)

    // then
    resultPlans should be(empty)
  }

  test("plan argument for skipped ids when not all pattern nodes are skipped") {
    val context = newMockedLogicalPlanningContext(newMockedPlanContext())

    val qg = QueryGraph(
      argumentIds = Set(v"a", v"b", v"c"),
      patternNodes = Set(v"a", v"b", v"d")
    )

    argumentLeafPlanner(Set(v"b"))(qg, InterestingOrderConfig.empty, context) should equal(
      Set(Argument(Set(v"a", v"b", v"c")))
    )
  }
}
