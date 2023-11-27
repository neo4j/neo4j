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

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckableExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.util.InputPosition

case class NestedPlanCollectExpression(
  override val plan: LogicalPlan,
  projection: Expression,
  // We cannot put the actual pattern expression in the case class, that would lead to endless recursion
  // while trying to rewrite such pattern expressions away.
  override val solvedExpressionAsString: String
)(val position: InputPosition) extends NestedPlanExpression

case class NestedPlanExistsExpression(
  override val plan: LogicalPlan,
  // We cannot put the actual exists pattern expression in the case class, that would lead to endless recursion
  // while trying to rewrite such exists expressions away.
  override val solvedExpressionAsString: String
)(val position: InputPosition) extends NestedPlanExpression

case class NestedPlanGetByNameExpression(
  override val plan: LogicalPlan,
  columnNameToGet: LogicalVariable,
  // We cannot put the actual pattern expression in the case class, that would lead to endless recursion
  // while trying to rewrite such pattern expressions away.
  override val solvedExpressionAsString: String
)(val position: InputPosition) extends NestedPlanExpression

sealed abstract class NestedPlanExpression extends Expression with SemanticCheckableExpression {

  def plan: LogicalPlan

  /**
   * Used for rendering nicer plan descriptions.
   * @return
   */
  def solvedExpressionAsString: String

  override def semanticCheck(ctx: SemanticContext): SemanticCheck = SemanticCheck.success

  override def asCanonicalStringVal: String = solvedExpressionAsString

  override def isConstantForQuery: Boolean = false
}

object NestedPlanExpression {
  private val stringifier = ExpressionStringifier(_.asCanonicalStringVal)

  def exists(plan: LogicalPlan, solvedExpression: Expression)(position: InputPosition): NestedPlanExistsExpression =
    NestedPlanExistsExpression(plan, stringifier(solvedExpression))(position)

  def collect(
    plan: LogicalPlan,
    projection: Expression,
    solvedExpression: Expression
  )(position: InputPosition): NestedPlanCollectExpression =
    NestedPlanCollectExpression(plan, projection, stringifier(solvedExpression))(position)

  def count(
    plan: LogicalPlan,
    countVariable: LogicalVariable,
    solvedExpression: Expression
  )(position: InputPosition): NestedPlanGetByNameExpression =
    NestedPlanGetByNameExpression(
      plan,
      countVariable,
      stringifier(solvedExpression)
    )(position)
}
