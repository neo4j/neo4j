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

trait Projections {
  def projections: Map[String, Expression]
  def sortItems: Seq[SortItem]
  def limit: Option[Expression]
  def skip: Option[Expression]

  def withSkip(skip: Option[Expression]): Projections
  def withLimit(limit: Option[Expression]): Projections
  def withSortItems(sortItems: Seq[SortItem]): Projections
  def withProjections(projections: Map[String, Expression]): Projections

  def addProjections(projections: Map[String, Expression]): Projections =
    withProjections(projections ++ this.projections)

  def ++(other: Projections): Projections =
    Projections(
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

object Projections {
  def apply(projections: Map[String, Expression] = Map.empty,
            sortItems: Seq[SortItem] = Seq.empty,
            limit: Option[Expression] = None,
            skip: Option[Expression] = None) = ProjectionsImpl(projections, sortItems, limit, skip)

}

case class ProjectionsImpl(projections: Map[String, Expression], sortItems: Seq[SortItem], limit: Option[Expression],
                          skip: Option[Expression]) extends Projections{
  def withProjections(projections: Map[String, Expression]): Projections = copy(projections = projections)

  def withSortItems(sortItems: Seq[SortItem]): Projections = copy(sortItems = sortItems)

  def withSkip(skip: Option[Expression]): Projections = copy(skip = skip)

  def withLimit(limit: Option[Expression]): Projections = copy(limit = limit)
}
