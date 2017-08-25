/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_3.planner._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.{Ascending, LogicalPlanningContext, SortDescription}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.{LogicalPlan, Projection}
import org.neo4j.cypher.internal.frontend.v3_3.ast
import org.neo4j.cypher.internal.frontend.v3_3.ast.AscSortItem
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_3._

class ProjectionTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val x: ast.Expression = ast.UnsignedDecimalIntegerLiteral("110") _
  val y: ast.Expression = ast.UnsignedDecimalIntegerLiteral("10") _
  val variableSortItem: AscSortItem = ast.AscSortItem(ast.Variable("n") _) _
  val sortDescription: SortDescription = Ascending("n")

  test("should add projection for expressions not already covered") {
    // given
    val projections: Map[String, ast.Expression] = Map("42" -> ast.SignedDecimalIntegerLiteral("42") _)

    implicit val (context, startPlan) = queryGraphWith(projectionsMap = projections)

    // when
    val result = projection(startPlan, projections, distinct = false)

    // then
    result should equal(Projection(startPlan, projections)(solved))
    result.solved.horizon should equal(RegularQueryProjection(projections))
  }

  test("does not add projection when not needed") {
    // given
    val projections: Map[String, ast.Expression] = Map("n" -> ast.Variable("n") _)
    implicit val (context, startPlan) = queryGraphWith(projectionsMap = projections)

    // when
    val result = projection(startPlan, projections, distinct = false)

    // then
    result should equal(startPlan)
    result.solved.horizon should equal(RegularQueryProjection(projections))
  }

  test("does projection when renaming columns") {
    // given
    val projections: Map[String, ast.Expression] = Map("  n@34" -> ast.Variable("n") _)
    implicit val (context, startPlan) = queryGraphWith(projectionsMap = projections)

    // when
    val result = projection(startPlan, projections, distinct = false)

    // then
    result should equal(Projection(startPlan, projections)(solved))
    result.solved.horizon should equal(RegularQueryProjection(projections))
  }

  private def queryGraphWith(skip: Option[ast.Expression] = None,
                             limit: Option[ast.Expression] = None,
                             sortItems: Seq[ast.SortItem] = Seq.empty,
                             projectionsMap: Map[String, ast.Expression] = Map("n" -> ast.Variable("n")(pos))): (LogicalPlanningContext, LogicalPlan) = {
    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val ids = projectionsMap.keys.map(IdName(_)).toSet

    val plan =
      newMockedLogicalPlanWithSolved(ids, CardinalityEstimation.lift(RegularPlannerQuery(QueryGraph.empty.addPatternNodes(ids.toList: _*)), Cardinality(0)))

    (context, plan)
  }
}
