/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_1.{bottomUp, Rewriter}
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Return

object reattachAliasedExpressions extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = bottomUp(instance).apply(that)

  private val instance: Rewriter = Rewriter.lift {

    case r @ Return(_, ListedReturnItems(items: Seq[ReturnItem]), orderBy, _, _) =>
      r.copy(orderBy = orderBy.map(reattachOrderByExpressions(items)))(r.position)

    case r @ With(_, ListedReturnItems(items: Seq[ReturnItem]), orderBy, _, _, _) =>
      r.copy(orderBy = orderBy.map(reattachOrderByExpressions(items)))(r.position)
  }

  private def reattachOrderByExpressions(items: Seq[ReturnItem])(orderBy: OrderBy): OrderBy = {
    orderBy.copy(sortItems = orderBy.sortItems.map {
      (sortItem: SortItem) =>
        val returnItem = items.find(_.alias match {
          case Some(identifier) => identifier == sortItem.expression
          case None => false
        })
        returnItem match {
          case None => sortItem
          case Some(returnItem) => {
            sortItem match {
              case item: AscSortItem =>
                item.copy(expression = returnItem.expression)(item.position)
              case item: DescSortItem =>
                item.copy(expression = returnItem.expression)(item.position)
            }
          }
        }
    })(orderBy.position)
  }
}
