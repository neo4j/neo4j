/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Projection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.QueryPlan

class AggregationTest extends CypherFunSuite with LogicalPlanningTestSupport {
  val aggregatingMap: Map[String, Expression] = Map("count(*)" -> CountStar()(pos))

  test("should introduce aggregation when needed") {
    val projection = AggregatingQueryProjection(
      groupingKeys = Map.empty,
      aggregationExpressions = aggregatingMap,
      shuffle = QueryShuffle(
        sortItems = Seq.empty,
        limit = None,
        skip = None
      )
    )

    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val startPlan = newMockedQueryPlan()

    aggregation(startPlan, projection)(context) should equal(
      planAggregation(startPlan, Map(), aggregatingMap)
    )
  }

  test("RETURN x.prop, count(*) => WITH x.prop as `x.prop` RETURN `x.prop`, count(*)") {
    // Given RETURN x.prop, count(*) => WITH x.prop as `x.prop` RETURN `x.prop`, count(*)
    val groupingMap = Map("x.prop" -> Property(Identifier("x")(pos), PropertyKeyName("prop")(pos))(pos))
    val groupingKeyMap = Map("x.prop" -> Identifier("x.prop")(pos))
    val projection = AggregatingQueryProjection(
      groupingKeys = groupingKeyMap,
      aggregationExpressions = aggregatingMap
    )

    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val startPlan = newMockedQueryPlan()

    val solvedQuery = PlannerQuery(horizon = RegularQueryProjection(groupingMap))

    val projectionPlan = QueryPlan(
      plan = Projection(startPlan.plan, groupingMap),
      solved = solvedQuery
    )

    // When
    val result = aggregation(projectionPlan, projection)(context)

    // Then
    result should equal(
      planAggregation(
        left = projectionPlan,
        grouping = groupingKeyMap,
        aggregation = aggregatingMap)
    )
  }
}
