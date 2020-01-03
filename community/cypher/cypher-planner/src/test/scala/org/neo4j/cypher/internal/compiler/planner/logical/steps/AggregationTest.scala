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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner._
import org.neo4j.cypher.internal.ir.{AggregatingQueryProjection, InterestingOrder}
import org.neo4j.cypher.internal.logical.plans.{Aggregation, LogicalPlan, Projection}
import org.neo4j.cypher.internal.v4_0.expressions.CountStar
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class AggregationTest extends CypherFunSuite with LogicalPlanningTestSupport {
  private val aggregatingMap = Map("count(*)" -> CountStar()(pos))

  test("should introduce aggregation when needed") {
    val projection = AggregatingQueryProjection(
      groupingExpressions = Map.empty,
      aggregationExpressions = aggregatingMap
    )

    val context = newMockedLogicalPlanningContextWithFakeAttributes(
      planContext = newMockedPlanContext()
    )
    val startPlan = newMockedLogicalPlan()

    val result = aggregation(startPlan, projection, InterestingOrder.empty, context)
    result should equal(
      Aggregation(startPlan, Map(), aggregatingMap)
    )
  }

  test("should introduce variables when needed") {
    //match (n) return n.y, sum(n.x)
    val aggregationMap = Map("count(n.prop)" -> count(prop("n", "prop")))
    val groupingMap = Map("n.bar" -> prop("n", "bar"))
    val projectionPlan = AggregatingQueryProjection(
      groupingExpressions = groupingMap,
      aggregationExpressions = aggregationMap
    )

    val context = newMockedLogicalPlanningContextWithFakeAttributes(
      planContext = newMockedPlanContext()
    )

    val startPlan = newMockedLogicalPlan()

    val result = aggregation(startPlan, projectionPlan, InterestingOrder.empty, context)
    result should equal(
      Aggregation(
       startPlan, groupingMap, aggregationMap)
    )
  }

  test("RETURN x.prop, count(*) => WITH x.prop as `x.prop` RETURN `x.prop`, count(*)") {
    // Given RETURN x.prop, count(*) => WITH x.prop as `x.prop` RETURN `x.prop`, count(*)
    val groupingMap = Map("x.prop" -> prop("x", "prop"))
    val groupingKeyMap = Map("x.prop" -> varFor("x.prop"))
    val projection = AggregatingQueryProjection(
      groupingExpressions = groupingKeyMap,
      aggregationExpressions = aggregatingMap
    )

    val context = newMockedLogicalPlanningContextWithFakeAttributes(
      planContext = newMockedPlanContext()
    )

    val startPlan = newMockedLogicalPlan()

    val projectionPlan: LogicalPlan = Projection(startPlan, groupingMap)

    // When
    val result = aggregation(projectionPlan, projection, InterestingOrder.empty, context)
    // Then
    result should equal(
      Aggregation(projectionPlan, groupingKeyMap, aggregatingMap)
    )
  }
}
