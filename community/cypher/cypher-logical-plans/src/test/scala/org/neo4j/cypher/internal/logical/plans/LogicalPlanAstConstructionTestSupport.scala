/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport

trait LogicalPlanAstConstructionTestSupport extends AstConstructionTestSupport {
  val NL: String = System.lineSeparator()

  def nestedCollectExpr(
    plan: LogicalPlan,
    projection: String,
    solvedExpressionAsString: String
  ): NestedPlanCollectExpression =
    NestedPlanCollectExpression(
      plan,
      varFor(projection),
      solvedExpressionAsString.replaceAll("\n", NL)
    )(pos)

  def nestedExistsExpr(plan: LogicalPlan, solvedExpressionAsString: String): NestedPlanExistsExpression =
    NestedPlanExistsExpression(
      plan,
      solvedExpressionAsString.replaceAll("\n", NL)
    )(pos)

  def nestedGetColumnExpr(
    plan: LogicalPlan,
    columnVarName: String,
    solvedExpressionAsString: String
  ): NestedPlanGetByNameExpression =
    NestedPlanGetByNameExpression(
      plan,
      varFor(columnVarName),
      solvedExpressionAsString.replaceAll("\n", NL)
    )(pos)
}
