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

import org.neo4j.cypher.internal.compiler.v2_3.pipes.{Ascending, SortDescription}
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, LogicalPlanningContext}
import org.neo4j.cypher.internal.frontend.v2_3.ast
import org.neo4j.cypher.internal.frontend.v2_3.ast.{AscSortItem, PatternExpression}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class SortSkipAndLimitTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val x: ast.Expression = ast.UnsignedDecimalIntegerLiteral("110") _
  val y: ast.Expression = ast.UnsignedDecimalIntegerLiteral("10") _
  val identifierSortItem: AscSortItem = ast.AscSortItem(ast.Identifier("n") _) _
  val sortDescription: SortDescription = Ascending("n")

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  test("should add skip if query graph contains skip") {
    // given
    implicit val (query, context, startPlan) = queryGraphWith(
      skip = Some(x)
    )

    // when
    val result = sortSkipAndLimit(startPlan, query)

    // then
    result should equal(Skip(startPlan, x)(solved))
    result.solved.horizon should equal(RegularQueryProjection(Map.empty, QueryShuffle(skip = Some(x))))
  }

  test("should add limit if query graph contains limit") {
    // given
    implicit val (query, context, startPlan) = queryGraphWith(
      limit = Some(x)
    )

    // when
    val result = sortSkipAndLimit(startPlan, query)

    // then
    result should equal(Limit(startPlan, x)(solved))
    result.solved.horizon should equal(RegularQueryProjection(Map.empty, QueryShuffle(limit = Some(x))))
  }

  test("should add skip first and then limit if the query graph contains both") {
    // given
    implicit val (query, context, startPlan) = queryGraphWith(
      limit = Some(x),
      skip = Some(y)
    )

    // when
    val result = sortSkipAndLimit(startPlan, query)

    // then
    result should equal(Limit(Skip(startPlan, y)(solved), x)(solved))
    result.solved.horizon should equal(RegularQueryProjection(Map.empty, QueryShuffle(limit = Some(x), skip = Some(y))))
  }

  test("should add sort if query graph contains sort items") {
    // given
    implicit val (query, context, startPlan) = queryGraphWith(
      sortItems = Seq(identifierSortItem)
    )

    // when
    val result = sortSkipAndLimit(startPlan, query)

    // then
    result should equal(Sort(startPlan, Seq(sortDescription))(solved))

    result.solved.horizon should equal(RegularQueryProjection(Map.empty, QueryShuffle(sortItems = Seq(identifierSortItem))))
  }

  test("should add SortedLimit when query uses both ORDER BY and LIMIT") {
    val sortItems: Seq[AscSortItem] = Seq(identifierSortItem)
    // given
    implicit val (query, context, startPlan) = queryGraphWith(
      sortItems = sortItems,
      limit = Some(x)
    )

    // when
    val result = sortSkipAndLimit(startPlan, query)

    // then
    result should equal(
      SortedLimit(startPlan, x, sortItems)(solved)
    )
  }

  test("should add SortedLimit when query uses both ORDER BY and LIMIT, and add the SKIP value to the SortedLimit") {
    // given
    implicit val (query, context, startPlan) = queryGraphWith(
      sortItems = Seq(identifierSortItem),
      limit = Some(x),
      skip = Some(y)
    )

    // when
    val result = sortSkipAndLimit(startPlan, query)

    // then
    result should equal(
      Skip(SortedLimit(startPlan, ast.Add(x, y)(pos), Seq(identifierSortItem))(solved), y)(solved)
    )
  }

  private def queryGraphWith(skip: Option[ast.Expression] = None,
                             limit: Option[ast.Expression] = None,
                             sortItems: Seq[ast.SortItem] = Seq.empty,
                             projectionsMap: Map[String, ast.Expression] = Map("n" -> ast.Identifier("n")(pos))): (PlannerQuery, LogicalPlanningContext, LogicalPlan) = {
    val projection = RegularQueryProjection(
      projections = projectionsMap,
      shuffle = QueryShuffle(sortItems, skip, limit)
    )

    val qg = QueryGraph(patternNodes = Set(IdName("n")))
    val query = PlannerQuery(qg, projection)

    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val plan =
      newMockedLogicalPlanWithSolved(Set(IdName("n")),
        CardinalityEstimation.lift(PlannerQuery(QueryGraph.empty.addPatternNodes(IdName("n"))), Cardinality(0))
      )

    (query, context, plan)
  }
}
