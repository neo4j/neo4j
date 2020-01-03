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
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.ir._
import org.neo4j.cypher.internal.logical.plans.{LogicalPlan, Limit, Skip, DoNotIncludeTies}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class SkipAndLimitTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val x = literalUnsignedInt(110)
  private val y = literalUnsignedInt(10)

  test("should add skip if query graph contains skip") {
    // given
    val (query, context, startPlan) = queryGraphWith(
      Set("n"),
      solved("n"),
      projection = regularProjection(skip = Some(x)))

    // when
    val result = skipAndLimit(startPlan, query, context)

    // then
    result should equal(Skip(startPlan, x))
    context.planningAttributes.solveds.get(result.id).asSinglePlannerQuery.horizon should equal(
      RegularQueryProjection(Map.empty, QueryPagination(skip = Some(x)))
    )
  }

  test("should add limit if query graph contains limit") {
    // given
    val (query, context, startPlan) = queryGraphWith(
      Set("n"),
      solved("n"),
      projection = regularProjection(limit = Some(x)))

    // when
    val result = skipAndLimit(startPlan, query, context)

    // then
    result should equal(Limit(startPlan, x, DoNotIncludeTies))
    context.planningAttributes.solveds.get(result.id).asSinglePlannerQuery.horizon should equal(
      RegularQueryProjection(Map.empty, QueryPagination(limit = Some(x)))
    )
  }

  test("should add skip first and then limit if the query graph contains both") {
    // given
    val (query, context, startPlan) = queryGraphWith(
      Set("n"),
      solved("n"),
      projection = regularProjection(skip = Some(y), limit = Some(x)))

    // when
    val result = skipAndLimit(startPlan, query, context)

    // then
    result should equal(Skip(Limit(startPlan, add(x, y), DoNotIncludeTies), y))
    context.planningAttributes.solveds.get(result.id).asSinglePlannerQuery.horizon should equal(
      RegularQueryProjection(Map.empty, QueryPagination(limit = Some(x), skip = Some(y)))
    )
  }

  private def regularProjection(skip: Option[Expression] = None, limit: Option[Expression] = None) =
    RegularQueryProjection(projections = Map("n" -> varFor("n")), queryPagination = QueryPagination(skip, limit))

  private def solved(patternNodes: String*): SinglePlannerQuery =
    RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(patternNodes: _*))

  private def queryGraphWith(patternNodesInQG: Set[String],
                             solved: SinglePlannerQuery,
                             projection: QueryProjection = regularProjection(),
                             interestingOrder: InterestingOrder = InterestingOrder.empty):
  (RegularSinglePlannerQuery, LogicalPlanningContext, LogicalPlan) = {
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val qg = QueryGraph(patternNodes = patternNodesInQG)
    val query = RegularSinglePlannerQuery(queryGraph = qg, interestingOrder = interestingOrder, horizon = projection)

    val plan = newMockedLogicalPlanWithSolved(context.planningAttributes, idNames = patternNodesInQG, solved = solved)

    (query, context, plan)
  }
}
