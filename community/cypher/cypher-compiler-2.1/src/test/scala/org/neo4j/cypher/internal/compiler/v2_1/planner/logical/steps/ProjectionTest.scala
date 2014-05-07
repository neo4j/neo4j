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
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Expression, UnsignedIntegerLiteral, AscSortItem}
import org.neo4j.cypher.internal.compiler.v2_1.pipes.{Ascending, SortDescription}
import org.neo4j.cypher.internal.compiler.v2_1.functions.Collect

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
    val result = projection(startPlan)

    // then
    result.plan should equal(Skip(startPlan.plan, x))
    result.solved.skip should equal(Some(x))
  }

  test("should add limit if query graph contains limit") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      limit = Some(x)
    )

    // when
    val result = projection(startPlan)

    // then
    result.plan should equal(Limit(startPlan.plan, x))
    result.solved.limit should equal(Some(x))
  }

  test("should add skip first and then limit if the query graph contains both") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      limit = Some(x),
      skip = Some(y)
    )

    // when
    val result = projection(startPlan)

    // then
    result.plan should equal(Limit(Skip(startPlan.plan, y), x))
    result.solved.limit should equal(Some(x))
    result.solved.skip should equal(Some(y))
  }

  test("should add sort if query graph contains sort items") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      sortItems = Seq(identifierSortItem)
    )

    // when
    val result = projection(startPlan)

    // then
    result.plan should equal(Sort(startPlan.plan, Seq(sortDescription)))
    result.solved.sortItems should equal(Seq(identifierSortItem))
  }

  test("should add projection before sort if query graph contains sort items that are not identifiers") {
    // given
    val exp: ast.Expression = ast.Add(UnsignedIntegerLiteral("10") _, UnsignedIntegerLiteral("10") _) _
    val expressionSortItem: AscSortItem = ast.AscSortItem(exp) _

    implicit val (context, startPlan) = queryGraphWith(
      sortItems = Seq(expressionSortItem)
    )

    // when
    val result = projection(startPlan)

    // then
    result.solved.sortItems should equal(Seq(expressionSortItem))

    result.plan should equal(
      Projection(
        Sort(
          left = Projection(
            left = startPlan.plan,
            expressions = Map("  FRESHID0" -> exp, "n" -> ast.Identifier("n")_)
          ),
          sortItems = Seq(Ascending("  FRESHID0"))
        ),
        expressions = Map("n" -> ast.Identifier("n")_)
      )
    )
  }

  test("should add projection before sort with mixed identifier and non-identifier expressions") {
    // given
    val exp: ast.Expression = ast.Add(UnsignedIntegerLiteral("10") _, UnsignedIntegerLiteral("10") _) _
    val expressionSortItem: AscSortItem = ast.AscSortItem(exp) _

    implicit val (context, startPlan) = queryGraphWith(
      sortItems = Seq(expressionSortItem, identifierSortItem)
    )

    // when
    val result = projection(startPlan)

    // then
    result.solved.sortItems should equal(Seq(expressionSortItem, identifierSortItem))

    result.plan should equal(
      Projection(
        Sort(
          left = Projection(
            left = startPlan.plan,
            expressions = Map("  FRESHID0" -> exp, "n" -> ast.Identifier("n")_)
          ),
          sortItems = Seq(Ascending("  FRESHID0"), Ascending("n"))
        ),
        expressions = Map("n" -> ast.Identifier("n")_)
      )
    )
  }

  test("should add SortedLimit when query uses both ORDER BY and LIMIT") {
    val sortItems: Seq[AscSortItem] = Seq(identifierSortItem)
    // given
    implicit val (context, startPlan) = queryGraphWith(
      sortItems = sortItems,
      limit = Some(x)
    )

    // when
    val result = projection(startPlan)

    // then
    result.plan should equal(
      SortedLimit(startPlan.plan, x, sortItems)
    )

    result.solved.limit should equal(Some(x))
    result.solved.sortItems should equal(sortItems)
  }

  test("should add SortedLimit when query uses both ORDER BY and LIMIT, and add the SKIP value to the SortedLimit") {
    // given
    implicit val (context, startPlan) = queryGraphWith(
      sortItems = Seq(identifierSortItem),
      limit = Some(x),
      skip = Some(y)
    )

    // when
    val result = projection(startPlan)

    // then
    result.plan should equal(
      Skip(
        SortedLimit(
          startPlan.plan,
          ast.Add(x, y)_,
          Seq(identifierSortItem)
        ),
        y
      )
    )

    result.solved.limit should equal(Some(x))
    result.solved.skip should equal(Some(y))
    result.solved.sortItems should equal(Seq(identifierSortItem))
  }

  test("should add projection for expressions not already covered") {
    // given
    val projections: Map[String, ast.Expression] = Map("42" -> ast.SignedIntegerLiteral("42") _)

    implicit val (context, startPlan) = queryGraphWith(
      projections = projections
    )

    // when
    val result = projection(startPlan)

    // then
    result.plan should equal(Projection(startPlan.plan, projections))
    result.solved.projections should equal(projections)
  }

  test("should add projection without loosing aggregation data") {
    // given
    val aggregatingProjections = Map("c" -> Collect.invoke(ast.Identifier("n")(pos))(pos))

    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = QueryGraph(
        aggregatingProjections = aggregatingProjections
      )
    )

    val startPlan = QueryPlan(
      newMockedLogicalPlan("n")(context),
      QueryGraph.empty
        .addPatternNodes(IdName("n"))
        .withAggregatingProjections(aggregatingProjections)
    )

    // when
    val result = projection(startPlan)

    // then
    result.plan should equal(Projection(startPlan.plan, Map("c" -> ast.Identifier("c")_)))
    result.solved.aggregatingProjections should equal(aggregatingProjections)
    result.solved.projections should be(empty)
  }

  test("does not add projection when not needed") {
    // given
    val projections: Map[String, ast.Expression] = Map("n" -> ast.Identifier("n")_)
    implicit val (context, startPlan) = queryGraphWith(
      projections = projections
    )

    // when
    val result = projection(startPlan)

    // then
    result should equal(startPlan)
  }

  private def queryGraphWith(skip: Option[ast.Expression] = None,
                             limit: Option[ast.Expression] = None,
                             sortItems: Seq[ast.SortItem] = Seq.empty,
                             projections: Map[String, ast.Expression] = Map("n" -> ast.Identifier("n")(pos))): (LogicalPlanContext, QueryPlan) = {
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

    val plan = QueryPlan(
      newMockedLogicalPlan("n")(context),
      QueryGraph.empty.addPatternNodes(IdName("n"))
    )

    (context, plan)
  }


}
