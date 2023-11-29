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
package org.neo4j.cypher.internal.ir.ordering

import org.neo4j.cypher.internal.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.ast.IRExpression
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.projectExpression
import org.neo4j.cypher.internal.util.RewriterStopperWithParent
import org.neo4j.cypher.internal.util.RewriterWithParent
import org.neo4j.cypher.internal.util.topDownWithParent

/**
 * A column of either an [[OrderCandidate]] or a [[ProvidedOrder]].
 * Specifies both what is ordered and in what direction.
 */
sealed trait ColumnOrder {

  /**
   * @return the expression that this column is/should be ordered by.
   */
  def expression: Expression

  /**
   * @return `true` if ASC, `false` if DESC.
   */
  def isAscending: Boolean

  /**
   * @return projections needed to apply the sort of the expression
   */
  def projections: Map[LogicalVariable, Expression]

  /**
   * @return all dependencies from the  expression.
   *         The expression is first converted to the first expressions that was projected to form this expression.
   *         That way we get all the dependencies with "original" names.
   */
  def dependencies: Set[LogicalVariable] = {
    var currExpr = expression
    var prevExpr = projectExpression(expression, projections)
    while (currExpr != prevExpr) {
      currExpr = prevExpr
      prevExpr = projectExpression(currExpr, projections)
    }
    currExpr.dependencies
  }
}

object ColumnOrder {

  def unapply(arg: ColumnOrder): Some[Expression] = {
    Some(arg.expression)
  }

  def apply(expression: Expression, ascending: Boolean): ColumnOrder = {
    if (ascending) Asc(expression) else Desc(expression)
  }

  case class Asc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty)
      extends ColumnOrder {
    override val isAscending: Boolean = true
  }

  case class Desc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty)
      extends ColumnOrder {
    override val isAscending: Boolean = false
  }

  /**
   * Finds the original expression if it exists, otherwise returns the same expression.
   *
   * @param expression  the expression that potentially has been projected using projection
   * @param projections projections that might involve the expression.
   * @return the original expression, or the same expression.
   */
  def projectExpression(expression: Expression, projections: Map[LogicalVariable, Expression]): Expression = {
    expression.endoRewrite(topDownWithParent(
      RewriterWithParent.lift {
        // We must be careful when rewriting the logical variable used in a map projection
        case (v: LogicalVariable, Some(DesugaredMapProjection(mapVar, _, _))) if mapVar == v =>
          projections.get(v) match {
            // Replacing the variable with another logical variable is sound
            case Some(lv: LogicalVariable) => lv
            // Any other type is not
            case Some(_) => v
            // If v isn't a projected value, keep it as is
            case None => v
          }
        case (v: Variable, _) =>
          projections.getOrElse(v, v)
      },
      // Do not attempt rewriting in IR expressions, they contain variables in places where they cannot get substituted
      // by other expression.
      stopper = RewriterStopperWithParent(_.isInstanceOf[IRExpression])
    ))
  }
}
