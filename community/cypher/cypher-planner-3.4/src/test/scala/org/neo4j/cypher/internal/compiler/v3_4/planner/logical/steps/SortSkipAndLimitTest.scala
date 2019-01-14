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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_4.planner._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.frontend.v3_4.ast
import org.neo4j.cypher.internal.frontend.v3_4.ast.{AscSortItem, SortItem}
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans._

class SortSkipAndLimitTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val x: Expression = UnsignedDecimalIntegerLiteral("110") _
  val y: Expression = UnsignedDecimalIntegerLiteral("10") _
  val variableSortItem: AscSortItem = ast.AscSortItem(Variable("n") _) _
  val columnOrder: ColumnOrder = Ascending("n")

  private val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  test("should add skip if query graph contains skip") {
    // given
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWith(skip = Some(x))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    result should equal(Skip(startPlan, x))
    solveds.get(result.id).horizon should equal(RegularQueryProjection(Map.empty, QueryShuffle(skip = Some(x))))
  }

  test("should add limit if query graph contains limit") {
    // given
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWith(limit = Some(x))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    result should equal(Limit(startPlan, x, DoNotIncludeTies))
    solveds.get(result.id).horizon should equal(RegularQueryProjection(Map.empty, QueryShuffle(limit = Some(x))))
  }

  test("should add skip first and then limit if the query graph contains both") {
    // given
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWith(skip = Some(y), limit = Some(x))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    result should equal(Limit(Skip(startPlan, y), x, DoNotIncludeTies))
    solveds.get(result.id).horizon should equal(RegularQueryProjection(Map.empty, QueryShuffle(limit = Some(x), skip = Some(y))))
  }

  test("should add sort if query graph contains sort items") {
    // given
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWith(sortItems = Seq(variableSortItem))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    result should equal(Sort(startPlan, Seq(columnOrder)))

    solveds.get(result.id).horizon should equal(RegularQueryProjection(Map.empty, QueryShuffle(sortItems = Seq(variableSortItem))))
  }

  test("should add the correct plans when query uses both ORDER BY, SKIP and LIMIT") {
    // given
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWith(skip = Some(y), limit = Some(x), sortItems = Seq(variableSortItem))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    val sorted = Sort(startPlan, Seq(columnOrder))
    val skipped = Skip(sorted, y)
    val limited = Limit(skipped, x, DoNotIncludeTies)

    result should equal(limited)
  }

  private def queryGraphWith(skip: Option[Expression] = None,
                             limit: Option[Expression] = None,
                             sortItems: Seq[SortItem] = Seq.empty,
                             projectionsMap: Map[String, Expression] = Map("n" -> Variable("n")(pos))) = {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )
    val projection = RegularQueryProjection(
    projections = projectionsMap,
    shuffle = QueryShuffle(sortItems, skip, limit)
    )

    val qg = QueryGraph(patternNodes = Set("n"))
    val query = RegularPlannerQuery(queryGraph = qg, horizon = projection)


    val solved = RegularPlannerQuery(QueryGraph.empty.addPatternNodes("n"))
    val plan = newMockedLogicalPlanWithSolved(idNames = Set("n"), solveds = solveds, solved = solved)

    (query, context, plan, solveds, cardinalities)
  }
}
