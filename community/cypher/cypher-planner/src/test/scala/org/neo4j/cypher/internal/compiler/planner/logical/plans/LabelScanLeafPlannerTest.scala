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

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.labelScanLeafPlanner
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LabelScanLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val n = v"n"
  private val labelId = LabelId(12)

  private val qg = QueryGraph(
    selections = Selections(Set(Predicate(Set(n), hasLabels(n, "Awesome")))),
    patternNodes = Set(n)
  )

  test("simple label scan without compile-time label id") {
    // given
    val semanticTable = new SemanticTable()

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = semanticTable)

    // when
    val resultPlans = labelScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Set(
      NodeByLabelScan(n, labelName("Awesome"), Set.empty, IndexOrderNone)
    ))
  }

  test("simple label scan with a compile-time label ID") {
    // given
    val semanticTable: SemanticTable = newMockedSemanticTable
    when(semanticTable.id(labelName("Awesome"))).thenReturn(Some(labelId))

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = semanticTable)

    // when
    val resultPlans = labelScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Set(
      NodeByLabelScan(n, labelName("Awesome"), Set.empty, IndexOrderNone)
    ))
  }

  test("should not plan label scan for skipped id") {
    // given
    val semanticTable: SemanticTable = newMockedSemanticTable
    when(semanticTable.id(labelName("Awesome"))).thenReturn(Some(labelId))

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = semanticTable)

    // when
    val resultPlans = labelScanLeafPlanner(Set(v"n"))(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should be(empty)
  }
}
