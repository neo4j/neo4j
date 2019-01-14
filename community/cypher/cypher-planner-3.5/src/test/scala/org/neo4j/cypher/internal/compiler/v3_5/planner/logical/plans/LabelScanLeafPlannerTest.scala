/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps.labelScanLeafPlanner
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.v3_5.logical.plans.{LogicalPlan, NodeByLabelScan}
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.util.{Cost, LabelId}

class LabelScanLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val statistics: HardcodedGraphStatistics.type = hardcodedStatistics

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  test("simple label scan without compile-time label id") {
    // given
    val idName = "n"
    val hasLabels = HasLabels(Variable("n")_, Seq(LabelName("Awesome")_))_
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(idName), hasLabels))),
      patternNodes = Set(idName))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: NodeByLabelScan => Cost(1)
      case _                  => Cost(Double.MaxValue)
    })

    val semanticTable = new SemanticTable()

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext, metrics = factory.newMetrics(statistics, mock[ExpressionEvaluator], config), semanticTable = semanticTable)

    // when
    val resultPlans = labelScanLeafPlanner(qg, InterestingOrder.empty, context)

    // then
    resultPlans should equal(Seq(
      NodeByLabelScan(idName, lblName("Awesome"), Set.empty))
    )
  }

  test("simple label scan with a compile-time label ID") {
    // given
    val idName = "n"
    val labelId = LabelId(12)
    val labelName = LabelName("Awesome")(pos)
    val hasLabels = HasLabels(Variable("n")_, Seq(labelName))_
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(idName), hasLabels))),
      patternNodes = Set(idName))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: NodeByLabelScan => Cost(100)
      case _                  => Cost(Double.MaxValue)
    })

    implicit val semanticTable = newMockedSemanticTable
    when(semanticTable.id(labelName)).thenReturn(Some(labelId))

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext, metrics = factory.newMetrics(statistics, mock[ExpressionEvaluator], config), semanticTable = semanticTable)

    // when
    val resultPlans = labelScanLeafPlanner(qg, InterestingOrder.empty, context)

    // then
    resultPlans should equal(
      Seq(NodeByLabelScan(idName, lblName("Awesome"), Set.empty)))
  }
}
