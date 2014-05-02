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
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanContext
import org.neo4j.cypher.internal.compiler.v2_1.ast.{UnsignedIntegerLiteral, AscSortItem}
import org.neo4j.cypher.internal.compiler.v2_1.pipes.{Ascending, SortDescription}

class ProjectionTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val x: ast.Expression = ast.UnsignedIntegerLiteral("110") _
  val y: ast.Expression = ast.UnsignedIntegerLiteral("10") _
  val identifierSortItem: AscSortItem = ast.AscSortItem(ast.Identifier("n") _) _
  val sortDescription: SortDescription = Ascending("n")

  test("should add skip if query graph contains skip") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      skip = Some(x)
    )

    // when
    val result = projection(startPlan).plan

    // then
    result should equal(Skip(startPlan, x))
  }

  test("should add limit if query graph contains limit") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      limit = Some(x)
    )

    // when
    val result = projection(startPlan).plan

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
    val result = projection(startPlan).plan

    // then
    result should equal(Limit(Skip(startPlan, y), x))
  }

  test("should add sort if query graph contains sort items") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      sortItems = Seq(identifierSortItem)
    )

    // when
    val result = projection(startPlan).plan

    // then
    result should equal(Sort(startPlan, Seq(sortDescription))(Seq(identifierSortItem)))
  }

  test("should add projection before sort if query graph contains sort items that are not identifiers") {
    // given
    val exp: ast.Expression = ast.Add(UnsignedIntegerLiteral("10") _, UnsignedIntegerLiteral("10") _) _
    val expressionSortItem: AscSortItem = ast.AscSortItem(exp) _

    implicit val (context, startPlan) = queryGraphWith(
      sortItems = Seq(expressionSortItem)
    )

    // when
    val result = projection(startPlan).plan

    // then
    val expectedPlan: LogicalPlan = Sort(
      Projection(
        startPlan,
        expressions = Map("  FRESHID0" -> exp, "n" -> ast.Identifier("n")(pos)),
        hideProjections = true
      ),
      sortItems = Seq(Ascending("  FRESHID0"))
    )(Seq(expressionSortItem))

    result should equal(expectedPlan)
  }

  test("should add projection before sort with mixed identifier and non-identifier expressions") {
    // given
    val exp: ast.Expression = ast.Add(UnsignedIntegerLiteral("10") _, UnsignedIntegerLiteral("10") _) _
    val expressionSortItem: AscSortItem = ast.AscSortItem(exp) _

    implicit val (context, startPlan) = queryGraphWith(
      sortItems = Seq(expressionSortItem, identifierSortItem)
    )

    // when
    val result = projection(startPlan).plan

    // then
    val expectedPlan: LogicalPlan = Sort(
      Projection(
        startPlan,
        expressions = Map("  FRESHID0" -> exp, "n" -> ast.Identifier("n") _),
        hideProjections = true
      ),
      sortItems = Seq(Ascending("  FRESHID0"), Ascending("n"))
    )(Seq(expressionSortItem))

    result should equal(expectedPlan)
  }

  test("should add SortedLimit when query uses both ORDER BY and LIMIT") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      sortItems = Seq(identifierSortItem),
      limit = Some(x)
    )

    // when
    val result = projection(startPlan).plan

    // then
    result should equal(SortedLimit(startPlan, x, Seq[ast.SortItem](identifierSortItem))(x))
  }

  test("should add SortedLimit when query uses both ORDER BY and LIMIT, and add the SKIP value to the SortedLimit") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      sortItems = Seq(identifierSortItem),
      limit = Some(x),
      skip = Some(y)
    )

    // when
    val result = projection(startPlan).plan

    // then
    result should equal(Skip(SortedLimit(startPlan, ast.Add(x, y)(pos), Seq(identifierSortItem))(x), y))
  }

  test("should add projection for expressions not already covered") {
    // given
    val projections: Map[String, ast.Expression] = Map("42" -> ast.SignedIntegerLiteral("42") _)

    implicit val (context, startPlan) = queryGraphWith(
      projections = projections
    )

    // when
    val result = projection(startPlan).plan

    // then
    result should equal(Projection(startPlan, projections))
  }

  test("does not add projection when not needed") {
    // given
    val projections: Map[String, ast.Expression] = Map("n" -> ast.Identifier("n") _)
    implicit val (context, startPlan) = queryGraphWith(
      projections = projections
    )

    // when
    val result = projection(startPlan).plan

    // then
    result should equal(startPlan)
  }

  private def queryGraphWith(skip: Option[ast.Expression] = None,
                             limit: Option[ast.Expression] = None,
                             sortItems: Seq[ast.SortItem] = Seq.empty,
                             projections: Map[String, ast.Expression] = Map("n" -> ast.Identifier("n")(pos))): (LogicalPlanContext, LogicalPlan) = {
    val qg = QueryGraph(
      limit = limit,
      skip = skip,
      sortItems = sortItems,
      projections = projections,
      patternNodes = Set(IdName("n")))

    val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg
    )

    (context, newMockedLogicalPlan("n")(context))
  }


}
