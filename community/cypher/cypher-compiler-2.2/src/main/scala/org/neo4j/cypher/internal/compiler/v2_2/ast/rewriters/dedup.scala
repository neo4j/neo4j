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

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.SemanticTable
import org.neo4j.cypher.internal.compiler.v2_2.Rewritable._
import org.neo4j.cypher.internal.compiler.v2_2.Foldable._

case class dedup(table: SemanticTable) extends Rewriter {

  def apply(v1: AnyRef) = rewriteTopDown.apply(v1)

  private val topDownButNoAliases: Rewriter = Rewriter.lift(rewriteIdentifiers() orElse {
    case returnItem@AliasedReturnItem(e, id) =>
      val newExpression: Expression = e.endoRewrite(topDownButNoAliases)
      returnItem.copy(expression = newExpression)(returnItem.position)

    case x =>
      x.dup(x.children.map(t => topDownButNoAliases.apply(t).get).toList)
  })

  private val rewriteTopDown: Rewriter = Rewriter.lift(rewriteIdentifiers() orElse {
    case ret@Return(_, lst@ListedReturnItems(items), orderBy, skip, limit) =>
      val newItems: Seq[ReturnItem] = items.map {
        case returnItem@AliasedReturnItem(e, id) =>
          returnItem.copy(expression = e.endoRewrite(topDownButNoAliases))(returnItem.position)
        case item =>
          item
      }
      val returnItems = lst.copy(items = newItems)(lst.position)

      val aliases: Set[String] = newItems.collect {
        case returnItem@AliasedReturnItem(e, id) => id.name
      }.toSet

      val rewriter = bottomUp(Rewriter.lift(rewriteIdentifiers(aliases)))

      val newOrderBy = orderBy.map(_.endoRewrite(rewriter))
      val newSkip = skip.map(_.endoRewrite(rewriter))
      val newLimit = limit.map(_.endoRewrite(rewriter))

      ret.copy(
        returnItems = returnItems,
        orderBy = newOrderBy,
        skip = newSkip,
        limit = newLimit
      )(ret.position)

    case x =>
      x.dup(x.children.map(_.endoRewrite(rewriteTopDown)).toList)
  })

  private def rewriteIdentifiers(identifiersToIgnore: Set[String] = Set.empty): PartialFunction[AnyRef, AnyRef] = {
    case id@Identifier(name) if !identifiersToIgnore(name) =>
      val newName = name + table.symbols(id).offset.toString
      Identifier(newName)(id.position)
  }
}
