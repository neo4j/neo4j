
/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.AstNode
import pipes.QueryState
import symbols._

case class SimpleCase(expression: Expression, alternatives: Seq[(Expression, Expression)], default: Option[Expression])
  extends NullInNullOutExpression(expression) {

  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = {
    val matchingExpression = alternatives find {
      case (exp, res) => exp(m) == value
    } map (_._2)

    matchingExpression match {
      case Some(result) => result(m)
      case None         => default.getOrElse(Null()).apply(m)
    }
  }

  private def alternativeComparison = alternatives.map(_._1)

  private def alternativeExpressions = alternatives.map(_._2)

  def arguments = (expression +: (alternativeComparison ++ alternativeExpressions)).distinct

  protected def calculateType(symbols: SymbolTable): CypherType =
    calculateUpperTypeBound(AnyType(), symbols, alternativeExpressions ++ default.toSeq)

  def rewrite(f: (Expression) => Expression): Expression = {
    val newAlternatives = alternatives map {
      case (a, b) => (a.rewrite(f), b.rewrite(f))
    }

    f(SimpleCase(expression.rewrite(f), newAlternatives, default.map(f)))
  }

  def symbolTableDependencies: Set[String] = {
    val expressions = default.toSeq ++ alternativeComparison ++ alternativeExpressions :+ expression
    expressions.flatMap(_.symbolTableDependencies).toSet
  }
}
