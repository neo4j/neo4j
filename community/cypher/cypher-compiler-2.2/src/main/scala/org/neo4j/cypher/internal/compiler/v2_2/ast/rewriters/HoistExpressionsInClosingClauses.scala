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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.{bottomUp, Rewriter}

object hoistExpressionsInClosingClauses extends Rewriter {
  def apply(in: AnyRef): Option[AnyRef] = bottomUp(findingRewriter).apply(in)

  private val findingRewriter: Rewriter = Rewriter.lift {
    case r@Return(distinct, returnItems @ ListedReturnItems(items), orderBy, _, _) if returnItems.containsAggregate || distinct =>
      val innerRewriter = expressionRewriter(items)
      r.copy(
        orderBy = r.orderBy.endoRewrite(innerRewriter)
      )(r.position)

    case w@With(distinct, returnItems @ ListedReturnItems(items), orderBy, _, _, where) if returnItems.containsAggregate || distinct =>
      val innerRewriter = expressionRewriter(items)
      w.copy(
        orderBy = w.orderBy.endoRewrite(innerRewriter),
        where = w.where.endoRewrite(innerRewriter)
      )(w.position)
  }

  private def expressionRewriter(items: Seq[ReturnItem]): Rewriter = {
    val evaluatedExpressions: Map[Expression, String] = items.map { returnItem =>
      (returnItem.expression, returnItem.name)
    }.toMap
    val aliases: Seq[Identifier] = evaluatedExpressions.map { case (expr, name) =>
      Identifier(name)(expr.position)
    }.toSeq

    bottomUp(Rewriter.lift {
      case expression: Expression if !expression.exists(PartialFunction(aliases.contains)) =>
        val aliasOpt: Option[String] = evaluatedExpressions.get(expression)
        val aliasIdentitifier: Option[Identifier] = aliasOpt.map(Identifier(_)(expression.position))

        aliasIdentitifier.getOrElse(expression)
    })
  }
}
