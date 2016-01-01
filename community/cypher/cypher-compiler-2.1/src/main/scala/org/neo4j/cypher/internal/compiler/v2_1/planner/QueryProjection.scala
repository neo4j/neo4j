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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_1.docbuilders.internalDocBuilder
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier

sealed trait QueryHorizon

sealed abstract class QueryProjection extends QueryHorizon with internalDocBuilder.AsPrettyToString {
  def projections: Map[String, Expression]
  def shuffle: QueryShuffle
  def keySet: Set[String]
  def updateShuffle(f: QueryShuffle => QueryShuffle) = withShuffle(f(shuffle))
  def withProjections(projections: Map[String, Expression]): QueryProjection
  def withShuffle(shuffle: QueryShuffle): QueryProjection
}

object QueryProjection {
  val empty = RegularQueryProjection()

  def forIds(coveredIds: Set[IdName]): Seq[AliasedReturnItem] =
    coveredIds.toSeq.map(idName =>
      AliasedReturnItem(Identifier(idName.name)(null), Identifier(idName.name)(null))(null))

  def combine(lhs: QueryProjection, rhs: QueryProjection): QueryProjection = (lhs, rhs) match {
    case (left: RegularQueryProjection, right: RegularQueryProjection) =>
      left ++ right

    case _ =>
      throw new InternalException("Aggregations cannot be combined")
  }
}

final case class QueryShuffle(sortItems: Seq[SortItem] = Seq.empty,
                              skip: Option[Expression] = None,
                              limit: Option[Expression] = None) extends internalDocBuilder.AsPrettyToString {

  def withSortItems(sortItems: Seq[SortItem]) = copy(sortItems = sortItems)
  def withSkip(skip: Option[Expression]) = copy(skip = skip)
  def withLimit(limit: Option[Expression]) = copy(limit = limit)

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

final case class RegularQueryProjection(projections: Map[String, Expression] = Map.empty,
                                        shuffle: QueryShuffle = QueryShuffle.empty) extends QueryProjection {
  def keySet: Set[String] = projections.keySet

  def ++(other: RegularQueryProjection) =
    RegularQueryProjection(
      projections = projections ++ other.projections,
      shuffle = shuffle ++ other.shuffle
    )

  override def withProjections(projections: Map[String, Expression]): RegularQueryProjection =
    copy(projections = projections)

  def withShuffle(shuffle: QueryShuffle) =
    copy(shuffle = shuffle)
}

final case class AggregatingQueryProjection(groupingKeys: Map[String, Expression] = Map.empty,
                                            aggregationExpressions: Map[String, Expression] = Map.empty,
                                            shuffle: QueryShuffle = QueryShuffle.empty) extends QueryProjection {

  assert(
    !(groupingKeys.isEmpty && aggregationExpressions.isEmpty),
    "Everything can't be empty"
  )

  def projections: Map[String, Expression] = groupingKeys

  def keySet: Set[String] = groupingKeys.keySet ++ aggregationExpressions.keySet

  override def withProjections(groupingKeys: Map[String, Expression]): AggregatingQueryProjection =
    copy(groupingKeys = groupingKeys)

  def withAggregatingExpressions(aggregationExpressions: Map[String, Expression]) =
    copy(aggregationExpressions = aggregationExpressions)

  def withShuffle(shuffle: QueryShuffle) =
    copy(shuffle = shuffle)
}

case class UnwindProjection(identifier: IdName, exp: Expression) extends QueryHorizon
