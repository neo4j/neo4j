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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.{LogicalPlanningTestSupport, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_1.ast
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Expression, AscSortItem, SortItem}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanContext

class PostProjectionTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val x: ast.Expression = ast.UnsignedIntegerLiteral("110") _
  val y: ast.Expression = ast.UnsignedIntegerLiteral("10") _
  val sortItem: AscSortItem = ast.AscSortItem(ast.Identifier("n") _) _

  private def queryGraphWith(skip: Option[Expression] = None, limit: Option[Expression] = None, sortItems: Seq[SortItem] = Seq.empty):
  (LogicalPlanContext, LogicalPlan) = {
    val qg = QueryGraph(
      limit = limit,
      skip = skip,
      sortItems = sortItems)

    val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg
    )

    (context, newMockedLogicalPlan("n")(context))
  }

  test("should add skip if query graph contains skip") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      skip = Some(x)
    )

    // when
    val result = postProjection(startPlan)

    // then
    result should equal(Skip(startPlan, x))
  }

  test("should add limit if query graph contains limit") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      limit = Some(x)
    )

    // when
    val result = postProjection(startPlan)

    // then
    result should equal(Limit(startPlan, x))
  }

  test("should add skip first and then limit if the query graph contains both") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      limit = Some(x),
      skip = Some(y)
    )

    // when
    val result = postProjection(startPlan)

    // then
    result should equal(Limit(Skip(startPlan, y), x))
  }

  test("should add sort if query graph contains sort items") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      sortItems = Seq(sortItem)
    )

    // when
    val result = postProjection(startPlan)

    // then
    result should equal(Sort(startPlan, Seq(sortItem)))
  }

  test("should add SortedLimit when query uses both ORDER BY and LIMIT") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      sortItems = Seq(sortItem),
      limit = Some(x)
    )

    // when
    val result = postProjection(startPlan)

    // then
    result should equal(SortedLimit(startPlan, x, Seq[SortItem](sortItem))(x))
  }

  test("should add SortedLimit when query uses both ORDER BY and LIMIT, and add the SKIP value to the SortedLimit") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      sortItems = Seq(sortItem),
      limit = Some(x),
      skip = Some(y)
    )

    // when
    val result = postProjection(startPlan)

    // then
    result should equal(Skip(SortedLimit(startPlan, ast.Add(x, y)(pos), Seq(sortItem))(x), y))
  }
}
