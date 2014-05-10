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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.ast.{SortItem, Expression}
import org.neo4j.cypher.InternalException

trait QueryProjection {
  def projections: Map[String, Expression]
  def sortItems: Seq[SortItem]
  def limit: Option[Expression]
  def skip: Option[Expression]

  def withSkip(skip: Option[Expression]): QueryProjection
  def withLimit(limit: Option[Expression]): QueryProjection
  def withSortItems(sortItems: Seq[SortItem]): QueryProjection
  def withProjections(projections: Map[String, Expression]): QueryProjection

  def addProjections(projections: Map[String, Expression]): QueryProjection =
    withProjections(projections ++ this.projections)

  def ++(other: QueryProjection): QueryProjection
}

object QueryProjection {
  def apply(projections: Map[String, Expression] = Map.empty,
            sortItems: Seq[SortItem] = Seq.empty,
            limit: Option[Expression] = None,
            skip: Option[Expression] = None) = QueryProjectionImpl(projections, sortItems, limit, skip)

  val empty = apply()
}

case object NoProjection extends QueryProjection {
  def projections = Map.empty
  def limit = None
  def sortItems = Vector.empty
  def skip = None

  def withLimit(limit: Option[Expression]) = thisIfEmpty(limit, QueryProjection.empty.withLimit)
  def withProjections(projections: Map[String, Expression]) = thisIfEmpty(projections, QueryProjection.empty.withProjections)
  def withSortItems(sortItems: Seq[SortItem]) = thisIfEmpty(sortItems, QueryProjection.empty.withSortItems)
  def withSkip(skip: Option[Expression]) = thisIfEmpty(skip, QueryProjection.empty.withSkip)

  private def thisIfEmpty[A <: { def isEmpty: Boolean }](element: A, f: A => QueryProjection):QueryProjection =
    if (element.isEmpty) this else f(element)

  def ++(other: QueryProjection): QueryProjection =
     withLimit(other.limit)
      .withProjections(other.projections)
      .withSkip(other.skip)
      .withSortItems(other.sortItems)
}

case class QueryProjectionImpl(projections: Map[String, Expression], sortItems: Seq[SortItem], limit: Option[Expression],
                          skip: Option[Expression]) extends QueryProjection{
  def withProjections(projections: Map[String, Expression]): QueryProjection = copy(projections = projections)

  def withSortItems(sortItems: Seq[SortItem]): QueryProjection = copy(sortItems = sortItems)

  def withSkip(skip: Option[Expression]): QueryProjection = copy(skip = skip)

  def withLimit(limit: Option[Expression]): QueryProjection = copy(limit = limit)

  def ++(other: QueryProjection): QueryProjection =
    QueryProjection(
      projections = projections ++ other.projections,
      sortItems = other.sortItems,
      limit = either(limit, other.limit),
      skip = either(skip, other.skip)
    )

  private def either[T](a: Option[T], b: Option[T]): Option[T] = (a, b) match {
    case (Some(_), Some(_)) => throw new InternalException("Can't join two query graphs with different SKIP")
    case (s@Some(_), None) => s
    case (None, s) => s
  }
}
