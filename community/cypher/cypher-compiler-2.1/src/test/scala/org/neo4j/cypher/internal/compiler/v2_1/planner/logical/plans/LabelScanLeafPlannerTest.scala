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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_1.LabelId
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Expression, LabelName, Identifier, HasLabels}
import org.neo4j.cypher.internal.compiler.v2_1.planner.{MainQueryGraph, LogicalPlanningTestSupport, QueryGraph, Selections}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.labelScanLeafPlanner
import org.mockito.Matchers._

class LabelScanLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val statistics = newMockedStatistics

  test("simple label scan without compile-time label id") {
    // given
    val idName = IdName("n")
    val projections: Map[String, Expression] = Map("n" -> Identifier("n")_)
    val hasLabels = HasLabels(Identifier("n")_, Seq(LabelName("Awesome")()_))_
    val qg = MainQueryGraph(projections, Selections(Seq(Set(idName) -> hasLabels)), Set(idName), Set.empty, Seq.empty)

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: NodeByLabelScan => 1
      case _                  => Double.MaxValue
    })
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg,
      metrics = factory.newMetrics(statistics)
    )

    // when
    val resultPlans = labelScanLeafPlanner(Map(idName -> Set(hasLabels)))()

    // then
    resultPlans should equal(Seq(NodeByLabelScan(idName, Left("Awesome"))()))
  }

  test("simple label scan with a compile-time label ID") {
    // given
    val idName = IdName("n")
    val projections: Map[String, Expression] = Map("n" -> Identifier("n")_)
    val labelId = LabelId(12)
    val hasLabels = HasLabels(Identifier("n")_, Seq(LabelName("Awesome")(Some(labelId))_))_
    val qg = MainQueryGraph(projections, Selections(Seq(Set(idName) -> hasLabels)), Set(idName), Set.empty, Seq.empty)

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: NodeByLabelScan => 100
      case _                  => Double.MaxValue
    })
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg,
      metrics = factory.newMetrics(statistics)
    )
    when(context.planContext.indexesGetForLabel(12)).thenReturn(Iterator.empty)

    // when
    val resultPlans = labelScanLeafPlanner(Map(idName -> Set(hasLabels)))()

    // then
    resultPlans should equal(Seq(NodeByLabelScan(idName, Right(labelId))()))
  }
}
