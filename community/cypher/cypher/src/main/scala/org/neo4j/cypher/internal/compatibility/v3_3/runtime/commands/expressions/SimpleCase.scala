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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState
import org.neo4j.values.AnyValue

case class SimpleCase(expression: Expression, alternatives: Seq[(Expression, Expression)], default: Option[Expression])
  extends Expression {

  def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val value = expression(ctx, state)

    val matchingExpression: Option[Expression] = alternatives collectFirst {
      case (exp, res) if exp(ctx, state) == value => res
    }

    matchingExpression match {
      case Some(resultExpression) => resultExpression(ctx, state)
      case None => default.getOrElse(Null()).apply(ctx, state)
    }
  }

  private def alternativeComparison = alternatives.map(_._1)

  private def alternativeExpressions = alternatives.map(_._2)

  def arguments = (expression +: (alternativeComparison ++ alternativeExpressions ++ default.map(Seq(_)).getOrElse(Seq()))).distinct

  def rewrite(f: (Expression) => Expression): Expression = {
    val newAlternatives = alternatives map {
      case (a, b) => (a.rewrite(f), b.rewrite(f))
    }

    f(SimpleCase(expression.rewrite(f), newAlternatives, default.map(f)))
  }

  def symbolTableDependencies: Set[String] = {
    val expressions = default.toIndexedSeq ++ alternativeComparison ++ alternativeExpressions :+ expression
    expressions.flatMap(_.symbolTableDependencies).toSet
  }
}
