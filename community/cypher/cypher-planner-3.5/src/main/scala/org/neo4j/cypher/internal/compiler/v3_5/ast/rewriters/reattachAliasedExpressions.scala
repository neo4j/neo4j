/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.ast.rewriters

import org.opencypher.v9_0.util.{Rewriter, bottomUp}
import org.opencypher.v9_0.ast.{Return, ReturnItem, With}
import org.opencypher.v9_0.expressions.{Expression, Variable}

case object reattachAliasedExpressions extends Rewriter {
  override def apply(in: AnyRef): AnyRef = findingRewriter.apply(in)

  private val findingRewriter: Rewriter = bottomUp(Rewriter.lift {
    case clause: Return =>
      val innerRewriter = expressionRewriter(clause.returnItems.items)
      clause.copy(
        orderBy = clause.orderBy.endoRewrite(innerRewriter)
      )(clause.position)

    case clause: With =>
      val innerRewriter = expressionRewriter(clause.returnItems.items)
      clause.copy(
        orderBy = clause.orderBy.endoRewrite(innerRewriter)
      )(clause.position)
  })

  private def expressionRewriter(items: Seq[ReturnItem]): Rewriter = {
    val aliasedExpressions: Map[String, Expression] = items.map { returnItem =>
      (returnItem.name, returnItem.expression)
    }.toMap

    bottomUp(Rewriter.lift {
      case id@Variable(name) if aliasedExpressions.contains(name) => aliasedExpressions(name)
    })
  }
}
