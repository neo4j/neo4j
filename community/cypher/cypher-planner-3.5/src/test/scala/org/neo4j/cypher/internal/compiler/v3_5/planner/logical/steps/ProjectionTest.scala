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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{LogicalPlanningContext, PlanMatchHelp}
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.v3_5.logical.plans.Ascending
import org.neo4j.cypher.internal.v3_5.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_5.logical.plans.Projection
import org.neo4j.cypher.internal.v3_5.ast
import org.neo4j.cypher.internal.v3_5.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.v3_5.ast.AscSortItem
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class ProjectionTest extends CypherFunSuite with LogicalPlanningTestSupport with PlanMatchHelp {

  val x: Expression = UnsignedDecimalIntegerLiteral("110") _
  val y: Expression = UnsignedDecimalIntegerLiteral("10") _
  val variableSortItem: AscSortItem = ast.AscSortItem(Variable("n") _) _
  val columnOrder: ColumnOrder = Ascending("n")

  test("should add projection for expressions not already covered") {
    // given
    val projections: Map[String, Expression] = Map("42" -> SignedDecimalIntegerLiteral("42") _)

    val (context, startPlan) = queryGraphWith(projectionsMap = projections)

    // when
    val result = projection(startPlan, projections, projections, InterestingOrder.empty, context)

    // then
    result should equal(Projection(startPlan, projections))
    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(projections))
  }

  test("should mark as solved according to projectionsToMarkSolved argument") {
    // given
    val projections: Map[String, Expression] = Map("42" -> SignedDecimalIntegerLiteral("42")(pos), "43" -> SignedDecimalIntegerLiteral("43")(pos))
    val projectionsToMarkSolved: Map[String, Expression] = Map("42" -> SignedDecimalIntegerLiteral("42")(pos))

    val (context, startPlan) = queryGraphWith(projectionsMap = projections)

    // when
    val result = projection(startPlan, projections, projectionsToMarkSolved, InterestingOrder.empty, context)

    // then
    result should equal(Projection(startPlan, projections))
    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(projectionsToMarkSolved))
  }

  test("does not add projection when not needed") {
    // given
    val projections: Map[String, Expression] = Map("n" -> Variable("n") _)
    val (context, startPlan) = queryGraphWith(projectionsMap = projections)

    // when
    val result = projection(startPlan, projections, projections, InterestingOrder.empty, context)

    // then
    result should equal(startPlan)
    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(projections))
  }

  test("only adds the set difference of projections needed") {
    // given
    val projections: Map[String, Expression] = Map("n" -> Variable("n") _, "42" -> SignedDecimalIntegerLiteral("42") _)
    val (context, startPlan) = queryGraphWith(projectionsMap = projections)

    // when
    val result = projection(startPlan, projections, projections, InterestingOrder.empty, context)

    // then
    val actualProjections = Map("42" -> SignedDecimalIntegerLiteral("42")(pos))
    result should equal(Projection(startPlan, actualProjections))
    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(projections))
  }

  test("does projection when renaming columns") {
    // given
    val projections: Map[String, Expression] = Map("  n@34" -> Variable("n") _)
    val (context, startPlan) = queryGraphWith(projectionsMap = projections)

    // when
    val result = projection(startPlan, projections, projections, InterestingOrder.empty, context)

    // then
    result should equal(Projection(startPlan, projections))
    context.planningAttributes.solveds.get(result.id).horizon should equal(RegularQueryProjection(projections))
  }

  private def queryGraphWith(skip: Option[Expression] = None,
                             limit: Option[Expression] = None,
                             sortItems: Seq[ast.SortItem] = Seq.empty,
                             projectionsMap: Map[String, Expression] = Map("n" -> Variable("n")(pos)),
                             availablePropertiesFromIndexes: Map[Property, String] = Map.empty):
  (LogicalPlanningContext, LogicalPlan) = {
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext, semanticTable = new SemanticTable(types = mock[ASTAnnotationMap[Expression, ExpressionTypeInfo]]))

    val ids = projectionsMap.keySet

    val plan =
      newMockedLogicalPlanWithSolved(context.planningAttributes, idNames = ids, solved = RegularPlannerQuery(QueryGraph.empty.addPatternNodes(ids.toList: _*)),
        availablePropertiesFromIndexes = availablePropertiesFromIndexes)

    (context, plan)
  }
}
