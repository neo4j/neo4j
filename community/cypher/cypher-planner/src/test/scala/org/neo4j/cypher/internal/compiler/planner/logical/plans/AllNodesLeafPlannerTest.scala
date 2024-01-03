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
import org.neo4j.cypher.internal.compiler.planner.logical.steps.allNodesLeafPlanner
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class AllNodesLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("simple all nodes scan") {
    // given
    val queryGraph = QueryGraph(patternNodes = Set(v"n"))

    val planContext = newMockedPlanContext()
    val context = newMockedLogicalPlanningContext(planContext = planContext)

    // when
    val resultPlans = allNodesLeafPlanner(Set.empty)(queryGraph, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Set(AllNodesScan(varFor("n"), Set.empty)))
  }

  test("should not plan all node scan for skipped id") {
    // given
    val queryGraph = QueryGraph(patternNodes = Set(v"n"))

    val planContext = newMockedPlanContext()
    val context = newMockedLogicalPlanningContext(planContext = planContext)

    // when
    val resultPlans = allNodesLeafPlanner(Set(v"n"))(queryGraph, InterestingOrderConfig.empty, context)

    // then
    resultPlans should be(empty)
  }
}
