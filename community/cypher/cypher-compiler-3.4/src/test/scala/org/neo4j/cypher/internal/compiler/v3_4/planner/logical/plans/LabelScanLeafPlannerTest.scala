/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans

import org.mockito.Mockito._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_4.planner._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps.labelScanLeafPlanner
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.util.v3_4.{Cost, LabelId}
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan, NodeByLabelScan}
import org.neo4j.cypher.internal.v3_4.expressions._

class LabelScanLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val statistics: HardcodedGraphStatistics.type = hardcodedStatistics

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  test("simple label scan without compile-time label id") {
    // given
    val idName = IdName("n")
    val hasLabels = HasLabels(Variable("n")_, Seq(LabelName("Awesome")_))_
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(idName), hasLabels))),
      patternNodes = Set(idName))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel()).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => plan match {
      case _: NodeByLabelScan => Cost(1)
      case _                  => Cost(Double.MaxValue)
    })

    val semanticTable = new SemanticTable()

    implicit val context = newMockedLogicalPlanningContext(
      semanticTable = semanticTable,
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics, mock[ExpressionEvaluator])
    )

    // when
    val resultPlans = labelScanLeafPlanner(qg)

    // then
    resultPlans should equal(Seq(
      NodeByLabelScan(idName, lblName("Awesome"), Set.empty)(solved))
    )
  }

  test("simple label scan with a compile-time label ID") {
    // given
    val idName = IdName("n")
    val labelId = LabelId(12)
    val labelName = LabelName("Awesome")(pos)
    val hasLabels = HasLabels(Variable("n")_, Seq(labelName))_
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(idName), hasLabels))),
      patternNodes = Set(idName))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel()).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => plan match {
      case _: NodeByLabelScan => Cost(100)
      case _                  => Cost(Double.MaxValue)
    })

    implicit val semanticTable = newMockedSemanticTable
    when(semanticTable.id(labelName)).thenReturn(Some(labelId))

    implicit val context = newMockedLogicalPlanningContext(
      semanticTable = semanticTable,
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics, mock[ExpressionEvaluator])
    )

    // when
    val resultPlans = labelScanLeafPlanner(qg)

    // then
    resultPlans should equal(
      Seq(NodeByLabelScan(idName, lblName("Awesome"), Set.empty)(solved)))
  }
}
