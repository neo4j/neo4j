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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.bottomUp.BottomUpRewriter

object hoistExpressionsInClosingClauses extends Rewriter {
  def apply(in: AnyRef): Option[AnyRef] = bottomUp(instance).apply(in)

  private val instance: Rewriter = Rewriter.lift {
    case r@Return(distinct, returnItems @ ListedReturnItems(items), orderBy, _, _) if returnItems.containsAggregate || distinct =>
      val innerRewriter = getInnerRewriter(items)
      r.copy(
        orderBy = r.orderBy.endoRewrite(innerRewriter)
      )(r.position)

    case w@With(distinct, returnItems @ ListedReturnItems(items), orderBy, _, _, where) if returnItems.containsAggregate || distinct =>
      val innerRewriter = getInnerRewriter(items)
      w.copy(
        orderBy = w.orderBy.endoRewrite(innerRewriter),
        where = w.where.endoRewrite(innerRewriter)
      )(w.position)
  }

  private def getInnerRewriter(items: Seq[ReturnItem]): BottomUpRewriter = {
    val evaluatedExpressions: Map[Expression, String] = items.map { returnItem =>
      (returnItem.expression, returnItem.name)
    }.toMap
    val aliases: Seq[Identifier] = evaluatedExpressions.map { case (expr, name) =>
      Identifier(name)(expr.position)
    }.toSeq

    bottomUp(Rewriter.lift {
      case expr: Expression if !expr.exists(PartialFunction(aliases.contains)) =>
        evaluatedExpressions.get(expr).map(Identifier(_)(expr.position)).getOrElse(expr)
    })
  }
}
