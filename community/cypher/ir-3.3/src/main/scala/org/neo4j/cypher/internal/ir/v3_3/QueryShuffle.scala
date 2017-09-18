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
package org.neo4j.cypher.internal.ir.v3_3

import org.neo4j.cypher.internal.frontend.v3_4.InternalException
import org.neo4j.cypher.internal.frontend.v3_4.ast.{Expression, Limit, Skip, SortItem}

final case class QueryShuffle(sortItems: Seq[SortItem] = Seq.empty,
                              skip: Option[Expression] = None,
                              limit: Option[Expression] = None) {

  def withSortItems(sortItems: Seq[SortItem]): QueryShuffle = copy(sortItems = sortItems)
  def withSkip(skip: Option[Skip]): QueryShuffle = copy(skip = skip.map(_.expression))
  def withSkipExpression(skip: Expression): QueryShuffle = copy(skip = Some(skip))
  def withLimit(limit: Option[Limit]): QueryShuffle = copy(limit = limit.map(_.expression))
  def withLimitExpression(limit: Expression): QueryShuffle = copy(limit = Some(limit))

  def ++(other: QueryShuffle): QueryShuffle =
    copy(
      sortItems = other.sortItems,
      limit = either("LIMIT", limit, other.limit),
      skip = either("SKIP", skip, other.skip)
    )

  private def either[T](what: String, a: Option[T], b: Option[T]): Option[T] = (a, b) match {
    case (Some(_), Some(_)) => throw new InternalException(s"Can't join two query shuffles with different $what")
    case (s@Some(_), None)  => s
    case (None, s)          => s
  }
}

object QueryShuffle {
  val empty = QueryShuffle()
}
