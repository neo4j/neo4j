/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.exceptions.InternalException

final case class QueryPagination(skip: Option[Expression] = None, limit: Option[Expression] = None) {

  def withSkip(skip: Option[Skip]): QueryPagination = copy(skip = skip.map(_.expression))
  def withSkipExpression(skip: Expression): QueryPagination = copy(skip = Some(skip))
  def withLimit(limit: Option[Limit]): QueryPagination = copy(limit = limit.map(_.expression))
  def withLimitExpression(limit: Expression): QueryPagination = copy(limit = Some(limit))

  def isEmpty: Boolean = skip.isEmpty && limit.isEmpty

  def ++(other: QueryPagination): QueryPagination =
    copy(
      limit = either("LIMIT", limit, other.limit),
      skip = either("SKIP", skip, other.skip)
    )

  private def either[T](what: String, a: Option[T], b: Option[T]): Option[T] = (a, b) match {
    case (Some(_), Some(_))  => throw new InternalException(s"Can't join two query pagination with different $what")
    case (s @ Some(_), None) => s
    case (None, s)           => s
  }
}

object QueryPagination {
  val empty = QueryPagination()
}
