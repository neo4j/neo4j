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
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.unionLabelScanLeafPlanner
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UnionLabelScanLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val variable = v"n"

  private val qg = QueryGraph(
    selections = Selections.from(ors(hasLabels(variable, "A"), hasLabels(variable, "B"))),
    patternNodes = Set(variable)
  )

  test("simple union label scan") {
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = new SemanticTable())

    // when
    val resultPlans = unionLabelScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Set(
      UnionNodeByLabelsScan(variable, Seq(labelName("A"), labelName("B")), Set.empty, IndexOrderNone)
    ))
  }

  test("no union label scan for different variables") {
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = new SemanticTable())
    val qg = QueryGraph(
      selections = Selections.from(ors(hasLabels("a", "A"), hasLabels("b", "B"))),
      patternNodes = Set(v"a", v"b")
    )

    // when
    val resultPlans = unionLabelScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should be(empty)
  }

  test("should not plan union label scan for skipped id") {
    // given
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = new SemanticTable())

    // when
    val resultPlans = unionLabelScanLeafPlanner(Set(v"n"))(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should be(empty)
  }
}
