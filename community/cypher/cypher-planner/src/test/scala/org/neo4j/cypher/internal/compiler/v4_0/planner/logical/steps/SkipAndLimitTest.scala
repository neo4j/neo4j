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
package org.neo4j.cypher.internal.compiler.v4_0.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v4_0.planner._
import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.ir.v4_0._
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class SkipAndLimitTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val x: Expression = UnsignedDecimalIntegerLiteral("110") _
  val y: Expression = UnsignedDecimalIntegerLiteral("10") _
  val sortVariable: Variable = Variable("n")(pos)
  val columnOrder: ColumnOrder = Ascending("n")
  val projectionsMap: Map[String, Expression] = Map("n" -> sortVariable)

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
    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(Map.empty, QueryPagination(skip = Some(x))))
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
    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(Map.empty, QueryPagination(limit = Some(x))))
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
    result should equal(Skip(Limit(startPlan, Add(x, y)(pos), DoNotIncludeTies), y))
    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(Map.empty, QueryPagination(limit = Some(x), skip = Some(y))))
  }

  private def regularProjection(skip: Option[Expression] = None, limit: Option[Expression] = None, projectionsMap: Map[String, Expression] = projectionsMap) =
    RegularQueryProjection(projections = projectionsMap, queryPagination = QueryPagination(skip, limit))

  private def solved(patternNodes: String*): PlannerQuery = RegularPlannerQuery(QueryGraph.empty.addPatternNodes(patternNodes: _*))

  private def queryGraphWith(patternNodesInQG: Set[String],
                             solved: PlannerQuery,
                             projection: QueryProjection = regularProjection(),
                             interestingOrder: InterestingOrder = InterestingOrder.empty):
  (RegularPlannerQuery, LogicalPlanningContext, LogicalPlan) = {
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext)

    val qg = QueryGraph(patternNodes = patternNodesInQG)
    val query = RegularPlannerQuery(queryGraph = qg, horizon = projection, interestingOrder = interestingOrder)

    val plan = newMockedLogicalPlanWithSolved(context.planningAttributes, idNames = patternNodesInQG, solved = solved)

    (query, context, plan)
  }
}
