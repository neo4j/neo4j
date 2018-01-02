/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{Aggregation, LogicalPlan, Projection}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class AggregationTest extends CypherFunSuite with LogicalPlanningTestSupport {
  val aggregatingMap: Map[String, Expression] = Map("count(*)" -> CountStar()(pos))

  val propExp: Expression = Property(ident("n"), PropertyKeyName("prop")(pos))(pos)
  val countExp: Expression = FunctionInvocation(FunctionName("count")(pos), propExp)(pos)
  val aggregatingMap2: Map[String, Expression] = Map("count(n.prop)" -> countExp)

  val propExp2: Expression = Property(ident("n"), PropertyKeyName("bar")(pos))(pos)
  val groupingMap: Map[String, Expression] = Map("n.bar" -> propExp2)

  test("should introduce aggregation when needed") {
    val projection = AggregatingQueryProjection(
      groupingKeys = Map.empty,
      aggregationExpressions = aggregatingMap
    )

    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val startPlan = newMockedLogicalPlan()

    aggregation(startPlan, projection)(context) should equal(
      Aggregation(startPlan, Map(), aggregatingMap)(solved)
    )
  }

  test("should introduce identifiers when needed") {
    //match n return n.y, sum(n.x)
    val projectionPlan = AggregatingQueryProjection(
      groupingKeys = groupingMap,
      aggregationExpressions = aggregatingMap2
    )

    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val startPlan = newMockedLogicalPlan()

    aggregation(startPlan, projectionPlan)(context) should equal(
      Aggregation(
        projection(startPlan, groupingMap + ("n" -> ident("n"))),
        groupingMap, aggregatingMap2)(solved)
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

    val startPlan = newMockedLogicalPlan()

    val projectionPlan: LogicalPlan = Projection(startPlan, groupingMap)(solved)

    // When
    val result = aggregation(projectionPlan, projection)(context)
    // Then
    result should equal(
      Aggregation(projectionPlan, groupingKeyMap, aggregatingMap)(solved)
    )
  }
}
