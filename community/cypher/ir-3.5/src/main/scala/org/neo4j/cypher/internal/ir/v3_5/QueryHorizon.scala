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
package org.neo4j.cypher.internal.ir.v3_5

import org.opencypher.v9_0.util.InternalException
import org.opencypher.v9_0.ast.AliasedReturnItem
import org.opencypher.v9_0.expressions.{Expression, StringLiteral, Variable}

trait QueryHorizon {

  def exposedSymbols(coveredIds: Set[String]): Set[String]

  def dependingExpressions: Seq[Expression]

  def preferredStrictness: Option[StrictnessMode]

  def dependencies: Set[String] = dependingExpressions.treeFold(Set.empty[String]) {
    case id: Variable =>
      acc => (acc + id.name, Some(identity))
  }

  def readOnly = true
}

final case class PassthroughAllHorizon() extends QueryHorizon {
  override def exposedSymbols(coveredIds: Set[String]): Set[String] = coveredIds

  override def dependingExpressions = Seq.empty

  override def preferredStrictness = None
}

case class UnwindProjection(variable: String, exp: Expression) extends QueryHorizon {
  override def exposedSymbols(coveredIds: Set[String]): Set[String] = coveredIds + variable

  override def dependingExpressions = Seq(exp)

  override def preferredStrictness = None
}

case class LoadCSVProjection(variable: String, url: Expression, format: CSVFormat, fieldTerminator: Option[StringLiteral]) extends QueryHorizon {
  override def exposedSymbols(coveredIds: Set[String]): Set[String] = coveredIds + variable

  override def dependingExpressions = Seq(url)

  override def preferredStrictness = None
}

sealed abstract class QueryProjection extends QueryHorizon {
  def projections: Map[String, Expression]
  def shuffle: QueryShuffle
  def keySet: Set[String]
  def withProjections(projections: Map[String, Expression]): QueryProjection
  def withShuffle(shuffle: QueryShuffle): QueryProjection

  override def dependingExpressions: Seq[Expression] = shuffle.sortItems.map(_.expression)
  override def preferredStrictness: Option[StrictnessMode] =
    if (shuffle.limit.isDefined && shuffle.sortItems.isEmpty) Some(LazyMode) else None

  def updateShuffle(f: QueryShuffle => QueryShuffle) = withShuffle(f(shuffle))
}

object QueryProjection {
  val empty = RegularQueryProjection()

  def forIds(coveredIds: Set[String]) =
    coveredIds.toIndexedSeq.map(idName =>
      AliasedReturnItem(Variable(idName)(null), Variable(idName)(null))(null))

  def combine(lhs: QueryProjection, rhs: QueryProjection): QueryProjection = (lhs, rhs) match {
    case (left: RegularQueryProjection, right: RegularQueryProjection) =>
      left ++ right

    case _ =>
      throw new InternalException("Aggregations cannot be combined")
  }
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

  override def exposedSymbols(coveredIds: Set[String]): Set[String] = projections.keySet

  override def dependingExpressions = super.dependingExpressions ++ projections.values
}

final case class AggregatingQueryProjection(groupingExpressions: Map[String, Expression] = Map.empty,
                                            aggregationExpressions: Map[String, Expression] = Map.empty,
                                            shuffle: QueryShuffle = QueryShuffle.empty) extends QueryProjection {

  assert(
    !(groupingExpressions.isEmpty && aggregationExpressions.isEmpty),
    "Everything can't be empty"
  )

  override def projections: Map[String, Expression] = groupingExpressions

  override def keySet: Set[String] = groupingExpressions.keySet ++ aggregationExpressions.keySet

  override def dependingExpressions = super.dependingExpressions ++ groupingExpressions.values ++ aggregationExpressions.values

  override def withProjections(groupingKeys: Map[String, Expression]): AggregatingQueryProjection =
    copy(groupingExpressions = groupingKeys)

  override def withShuffle(shuffle: QueryShuffle) =
    copy(shuffle = shuffle)

  override def exposedSymbols(coveredIds: Set[String]): Set[String] = groupingExpressions
    .keySet ++ aggregationExpressions.keySet
}

final case class DistinctQueryProjection(groupingKeys: Map[String, Expression] = Map.empty,
                                         shuffle: QueryShuffle = QueryShuffle.empty) extends QueryProjection {

  def projections: Map[String, Expression] = groupingKeys

  def keySet: Set[String] = groupingKeys.keySet

  override def dependingExpressions: Seq[Expression] = super.dependingExpressions ++ groupingKeys.values

  override def withProjections(groupingKeys: Map[String, Expression]): DistinctQueryProjection =
    copy(groupingKeys = groupingKeys)

  override def withShuffle(shuffle: QueryShuffle): DistinctQueryProjection =
    copy(shuffle = shuffle)

  override def exposedSymbols(coveredIds: Set[String]): Set[String] = groupingKeys.keySet
}
