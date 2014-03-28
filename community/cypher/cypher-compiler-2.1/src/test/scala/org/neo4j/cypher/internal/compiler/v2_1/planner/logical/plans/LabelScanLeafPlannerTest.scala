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
import org.neo4j.cypher.internal.compiler.v2_1.planner.{LogicalPlanningTestSupport, QueryGraph, Selections}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{Metrics, labelScanLeafPlanner}

class LabelScanLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("simple label scan without compile-time label id") {
    // given
    val idName = IdName("n")
    val projections: Map[String, Expression] = Map("n" -> Identifier("n")_)
    val hasLabels = HasLabels(Identifier("n")_, Seq(LabelName("Awesome")()_))_
    val qg = QueryGraph(projections, Selections(Seq(Set(idName) -> hasLabels)), Set(idName), Set.empty)

    implicit val context = newMockedLogicalPlanContext(queryGraph = qg,
      estimator = Metrics.newCardinalityEstimator {
        case _: NodeByLabelScan => 1
      })

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
    val qg = QueryGraph(projections, Selections(Seq(Set(idName) -> hasLabels)), Set(idName), Set.empty)

    implicit val context = newMockedLogicalPlanContext(queryGraph = qg,
      estimator = Metrics.newCardinalityEstimator {
        case _: NodeByLabelScan => 100
      })
    when(context.planContext.indexesGetForLabel(12)).thenReturn(Iterator.empty)

    // when
    val resultPlans = labelScanLeafPlanner(Map(idName -> Set(hasLabels)))()

    // then
    resultPlans should equal(Seq(NodeByLabelScan(idName, Right(labelId))()))
  }
}
