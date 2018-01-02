/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

case class SimpleCase(expression: Expression, alternatives: Seq[(Expression, Expression)], default: Option[Expression])
  extends Expression {

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val value = expression(ctx)

    val matchingExpression: Option[Expression] = alternatives collectFirst {
      case (exp, res) if exp(ctx) == value => res
    }

    matchingExpression match {
      case Some(resultExpression) => resultExpression(ctx)
      case None => default.getOrElse(Null()).apply(ctx)
    }
  }

  private def alternativeComparison = alternatives.map(_._1)

  private def alternativeExpressions = alternatives.map(_._2)

  def arguments = (expression +: (alternativeComparison ++ alternativeExpressions)).distinct

  protected def calculateType(symbols: SymbolTable): CypherType =
    calculateUpperTypeBound(CTAny, symbols, alternativeExpressions ++ default.toSeq)

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
