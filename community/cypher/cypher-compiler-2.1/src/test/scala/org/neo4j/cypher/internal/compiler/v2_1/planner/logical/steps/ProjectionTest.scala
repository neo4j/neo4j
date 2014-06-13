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
import org.neo4j.cypher.internal.compiler.v2_1.planner.{PlannerQuery, QueryProjection, LogicalPlanningTestSupport, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_1.ast
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v2_1.ast.{UnsignedIntegerLiteral, AscSortItem}
import org.neo4j.cypher.internal.compiler.v2_1.pipes.{Ascending, SortDescription}

class ProjectionTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val x: ast.Expression = ast.UnsignedIntegerLiteral("110") _
  val y: ast.Expression = ast.UnsignedIntegerLiteral("10") _
  val identifierSortItem: AscSortItem = ast.AscSortItem(ast.Identifier("n") _) _
  val sortDescription: SortDescription = Ascending("n")

  test("should add projection for expressions not already covered") {
    // given
    val projections: Map[String, ast.Expression] = Map("42" -> ast.SignedIntegerLiteral("42") _)

    implicit val (context, startPlan) = queryGraphWith(projectionsMap = projections)

    // when
    val result = projection(startPlan, projections)

    // then
    result.plan should equal(Projection(startPlan.plan, projections))
    result.solved.projection.projections should equal(projections)
  }

  test("does not add projection when not needed") {
    // given
    val projections: Map[String, ast.Expression] = Map("n" -> ast.Identifier("n") _)
    implicit val (context, startPlan) = queryGraphWith(projectionsMap = projections)

    // when
    val result = projection(startPlan, projections)

    // then
    result.plan should equal(startPlan.plan)
    result.solved.projection.projections should equal(projections)
  }

  private def queryGraphWith(skip: Option[ast.Expression] = None,
                             limit: Option[ast.Expression] = None,
                             sortItems: Seq[ast.SortItem] = Seq.empty,
                             projectionsMap: Map[String, ast.Expression] = Map("n" -> ast.Identifier("n")(pos))): (LogicalPlanningContext, QueryPlan) = {
//    val projections = QueryProjection(
//      limit = limit,
//      skip = skip,
//      sortItems = sortItems,
//      projections = projectionsMap)
//
//    val qg = QueryGraph(patternNodes = Set(IdName("n")))
//    val query = PlannerQuery(qg, projections)

    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val plan = QueryPlan(
      newMockedLogicalPlan("n"),
      PlannerQuery(QueryGraph.empty.addPatternNodes(IdName("n")))
    )

    (context, plan)
  }
}
