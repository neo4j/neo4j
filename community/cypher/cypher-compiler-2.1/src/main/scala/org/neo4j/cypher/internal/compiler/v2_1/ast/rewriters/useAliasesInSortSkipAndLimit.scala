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

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast.ListedReturnItems
import org.neo4j.cypher.internal.compiler.v2_1.ast.Return

object useAliasesInSortSkipAndLimit extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = bottomUp(instance).apply(that)

  private val instance: Rewriter = Rewriter.lift {
    case r@Return(_, ListedReturnItems(items), optOrderBy, optSkip, optLimit) if items.exists( (item) => containsAggregate(item.expression) ) =>
      val (newOrderBy, newSkip, newLimit) = rewriteInner(items, optOrderBy, optSkip, optLimit)

      r.copy(
        orderBy = newOrderBy,
        limit = newLimit,
        skip = newSkip
      )(r.position)

    case w@With(_, ListedReturnItems(items), optOrderBy, optSkip, optLimit, _) if items.exists( (item) => containsAggregate(item.expression) ) =>
      val (newOrderBy, newSkip, newLimit) = rewriteInner(items, optOrderBy, optSkip, optLimit)

      w.copy(
        orderBy = newOrderBy,
        limit = newLimit,
        skip = newSkip
      )(w.position)
  }

  private def rewriteInner(items: Seq[ReturnItem], optOrderBy: Option[OrderBy], optSkip: Option[Skip], optLimit: Option[Limit]) = {
    val rewriteMap = items.map(i => i.expression -> i.name).toMap
    val rewriter = topDown(Rewriter.lift {
      case e: Expression if rewriteMap.contains(e) =>
        Identifier(rewriteMap(e))(e.position)
    })

    val newOrderBy = optOrderBy.endoRewrite(rewriter)
    val newSkip = optSkip.endoRewrite(rewriter)
    val newLimit = optLimit.endoRewrite(rewriter)
    (newOrderBy, newSkip, newLimit)
  }

}
