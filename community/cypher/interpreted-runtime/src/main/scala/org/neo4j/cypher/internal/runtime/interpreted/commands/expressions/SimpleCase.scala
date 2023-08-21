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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue

case class SimpleCase(expression: Expression, alternatives: Seq[(Expression, Expression)], default: Option[Expression])
    extends Expression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val value = expression(row, state)

    val matchingExpression: Option[Expression] = alternatives collectFirst {
      case (exp, res) if exp(row, state).equalsWithNoValueCheck(value) => res
    }

    matchingExpression match {
      case Some(resultExpression) => resultExpression(row, state)
      case None                   => default.getOrElse(Null()).apply(row, state)
    }
  }

  private def alternativeComparison = alternatives.map(_._1)

  private def alternativeExpressions = alternatives.map(_._2)

  override def arguments: Seq[Expression] =
    (expression +: (alternativeComparison ++ alternativeExpressions ++ default)).distinct

  override def children: Seq[AstNode[_]] = expression +: (alternativeComparison ++ alternativeExpressions ++ default)

  override def rewrite(f: Expression => Expression): Expression = {
    val newAlternatives = alternatives map {
      case (a, b) => (a.rewrite(f), b.rewrite(f))
    }

    f(SimpleCase(expression.rewrite(f), newAlternatives, default.map(f)))
  }

}
