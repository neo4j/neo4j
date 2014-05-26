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

import org.neo4j.cypher.internal.compiler.v2_1.ast.{Identifier, SortItem, Expression}
import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.pprint.{GeneratedPretty, Pretty, pformat}

trait QueryProjection extends GeneratedPretty {
  def projections: Map[String, Expression]
  def sortItems: Seq[SortItem]
  def limit: Option[Expression]
  def skip: Option[Expression]
  def keySet = projections.keySet

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

  def forIds(coveredIds: Set[IdName]) =
    apply(coveredIds.toSeq.map( idName => idName.name -> Identifier(idName.name)(null)).toMap)

  val empty = apply(Map.empty)
}

case class QueryProjectionImpl(projections: Map[String, Expression], sortItems: Seq[SortItem], limit: Option[Expression],
                               skip: Option[Expression]) extends QueryProjection {
  def withProjections(projections: Map[String, Expression]): QueryProjection = copy(projections = projections)

  def withSortItems(sortItems: Seq[SortItem]): QueryProjection = copy(sortItems = sortItems)

  def withSkip(skip: Option[Expression]): QueryProjection = copy(skip = skip)

  def withLimit(limit: Option[Expression]): QueryProjection = copy(limit = limit)

  def ++(other: QueryProjection): QueryProjection =
    QueryProjection(
      projections = projections ++ other.projections,
      sortItems = other.sortItems,
      limit = either("LIMIT", limit, other.limit),
      skip = either("SKIP", skip, other.skip)
    )

  private def either[T](what: String, a: Option[T], b: Option[T]): Option[T] = (a, b) match {
    case (Some(_), Some(_)) => throw new InternalException(s"Can't join two query graphs with different $what")
    case (s@Some(_), None) => s
    case (None, s) => s
  }
}

case class AggregationProjection(groupingKeys: Map[String, Expression] = Map.empty,
                                 aggregationExpressions: Map[String, Expression] = Map.empty,
                                 sortItems: Seq[SortItem] = Seq.empty,
                                 limit: Option[Expression] = None,
                                 skip: Option[Expression] = None)
  extends QueryProjection {

  assert(
    !(groupingKeys.isEmpty && aggregationExpressions.isEmpty),
    "Everything can't be empty"
  )

  override def keySet: Set[String] = groupingKeys.keySet ++ aggregationExpressions.keySet

  def withSkip(skip: Option[Expression]) = copy(skip = skip)
  def withLimit(limit: Option[Expression]) = copy(limit = limit)
  def withProjections(projections: Map[String, Expression]) =
    throw new InternalException("Can't change type of projection")
  def withSortItems(sortItems: Seq[SortItem]) = copy(sortItems = sortItems)

  def ++(other: QueryProjection): QueryProjection =
    throw new InternalException("Aggregations cannot be combined")

  def projections: Map[String, Expression] = groupingKeys
}
