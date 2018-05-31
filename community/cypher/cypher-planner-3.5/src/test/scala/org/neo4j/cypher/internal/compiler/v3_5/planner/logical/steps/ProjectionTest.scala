/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.LogicalPlanningContext
import org.opencypher.v9_0.ast
import org.opencypher.v9_0.ast.AscSortItem
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.opencypher.v9_0.util.Cardinality
import org.opencypher.v9_0.expressions._
import org.neo4j.cypher.internal.v3_5.logical.plans.{Ascending, ColumnOrder, LogicalPlan, Projection}

class ProjectionTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val x: Expression = UnsignedDecimalIntegerLiteral("110") _
  val y: Expression = UnsignedDecimalIntegerLiteral("10") _
  val variableSortItem: AscSortItem = ast.AscSortItem(Variable("n") _) _
  val columnOrder: ColumnOrder = Ascending("n")

  test("should add projection for expressions not already covered") {
    // given
    val projections: Map[String, Expression] = Map("42" -> SignedDecimalIntegerLiteral("42") _)

    val (context, startPlan, solveds, cardinalities) = queryGraphWith(projectionsMap = projections)

    // when
    val result = projection(startPlan, projections, context, solveds, cardinalities)

    // then
    result should equal(Projection(startPlan, projections))
    solveds.get(result.id).horizon should equal(RegularQueryProjection(projections))
  }

  test("does not add projection when not needed") {
    // given
    val projections: Map[String, Expression] = Map("n" -> Variable("n") _)
    val (context, startPlan, solveds, cardinalities) = queryGraphWith(projectionsMap = projections)

    // when
    val result = projection(startPlan, projections, context, solveds, cardinalities)

    // then
    result should equal(startPlan)
    solveds.get(result.id).horizon should equal(RegularQueryProjection(projections))
  }

  test("does projection when renaming columns") {
    // given
    val projections: Map[String, Expression] = Map("  n@34" -> Variable("n") _)
    val (context, startPlan, solveds, cardinalities) = queryGraphWith(projectionsMap = projections)

    // when
    val result = projection(startPlan, projections, context, solveds, cardinalities)

    // then
    result should equal(Projection(startPlan, projections))
    solveds.get(result.id).horizon should equal(RegularQueryProjection(projections))
  }

  private def queryGraphWith(skip: Option[Expression] = None,
                             limit: Option[Expression] = None,
                             sortItems: Seq[ast.SortItem] = Seq.empty,
                             projectionsMap: Map[String, Expression] = Map("n" -> Variable("n")(pos))):
  (LogicalPlanningContext, LogicalPlan, Solveds, Cardinalities) = {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val ids = projectionsMap.keySet

    val plan =
      newMockedLogicalPlanWithSolved(solveds, cardinalities, idNames = ids, solved = RegularPlannerQuery(QueryGraph.empty.addPatternNodes(ids.toList: _*)))

    (context, plan, solveds, cardinalities)
  }
}
