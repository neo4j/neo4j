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
import org.neo4j.cypher.internal.compiler.planner.logical.steps.subtractionLabelScanLeafPlanner
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.SubtractionNodeByLabelsScan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SubtractionLabelScanLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val variable = v"n"

  private val qg = QueryGraph(
    selections = Selections.from(ands(
      hasLabels(variable, "A"),
      not(hasLabels(variable, "B")),
      hasLabels(variable, "C"),
      not(hasLabels(variable, "D"))
    )),
    patternNodes = Set(variable)
  )

  test("simple subtraction label scan") {
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = new SemanticTable())

    // when
    val resultPlans = subtractionLabelScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Set(
      SubtractionNodeByLabelsScan(
        variable,
        Seq(labelName("A"), labelName("C")),
        Seq(labelName("B"), labelName("D")),
        Set.empty,
        IndexOrderNone
      )
    ))
  }

  test("simple subtraction label scan descending") {
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = new SemanticTable())

    // when
    val resultPlans = subtractionLabelScanLeafPlanner(Set.empty)(
      qg,
      InterestingOrderConfig(InterestingOrder.required(RequiredOrderCandidate(Seq(Desc(variable))))),
      context
    )

    // then
    resultPlans should equal(Set(
      SubtractionNodeByLabelsScan(
        variable,
        Seq(labelName("A"), labelName("C")),
        Seq(labelName("B"), labelName("D")),
        Set.empty,
        IndexOrderDescending
      )
    ))
  }

  test("simple subtraction label scan ascending") {
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = new SemanticTable())

    // when
    val resultPlans = subtractionLabelScanLeafPlanner(Set.empty)(
      qg,
      InterestingOrderConfig(InterestingOrder.required(RequiredOrderCandidate(Seq(Asc(variable))))),
      context
    )

    // then
    resultPlans should equal(Set(
      SubtractionNodeByLabelsScan(
        variable,
        Seq(labelName("A"), labelName("C")),
        Seq(labelName("B"), labelName("D")),
        Set.empty,
        IndexOrderAscending
      )
    ))
  }

  test("no subtraction when no variable has both positive and negative labels") {
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = new SemanticTable())
    val qg = QueryGraph(
      selections = Selections.from(ands(
        hasLabels("n1", "A"),
        not(hasLabels("n2", "B")),
        hasLabels("n1", "C"),
        not(hasLabels("n2", "D"))
      )),
      patternNodes = Set(v"n1", v"n2")
    )

    // when
    val resultPlans = subtractionLabelScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should be(empty)
  }

  test("subtraction label scans for different variables") {
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = new SemanticTable())
    val qg = QueryGraph(
      selections = Selections.from(ands(
        hasLabels("n1", "A"),
        not(hasLabels("n1", "B")),
        hasLabels("n2", "C"),
        not(hasLabels("n2", "D"))
      )),
      patternNodes = Set(v"n1", v"n2")
    )

    // when
    val resultPlans = subtractionLabelScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Set(
      SubtractionNodeByLabelsScan(v"n1", Seq(labelName("A")), Seq(labelName("B")), Set.empty, IndexOrderNone),
      SubtractionNodeByLabelsScan(v"n2", Seq(labelName("C")), Seq(labelName("D")), Set.empty, IndexOrderNone)
    ))
  }

  test("should not plan subtraction label scan for skipped id") {
    // given
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = new SemanticTable())

    // when
    val resultPlans = subtractionLabelScanLeafPlanner(Set(v"n"))(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should be(empty)
  }
}
