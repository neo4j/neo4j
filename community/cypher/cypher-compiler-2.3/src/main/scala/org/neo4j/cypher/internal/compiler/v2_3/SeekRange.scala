/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3

sealed trait SeekRange[+V]

object InequalitySeekRange {
  def fromPartitionedBounds[V](bounds: Either[(Bounds[V], Option[Bounds[V]]), (Option[Bounds[V]], Bounds[V])]): InequalitySeekRange[V] =
    bounds match {
      case Left((lefts, None)) => RangeGreaterThan(lefts)
      case Left((lefts, Some(rights))) => RangeBetween(RangeGreaterThan(lefts), RangeLessThan(rights))
      case Right((None, rights)) => RangeLessThan(rights)
      case Right((Some(lefts), rights)) => RangeBetween(RangeGreaterThan(lefts), RangeLessThan(rights))
    }
}

sealed trait InequalitySeekRange[+V] extends SeekRange[V] {
  def mapBounds[P](f: V => P): InequalitySeekRange[P]

  def groupBy[K](f: Bound[V] => K): Map[K, InequalitySeekRange[V]]

  def includes[X >: V](value: X)(implicit ordering: MinMaxOrdering[X]): Boolean =
    inclusionTest[X](ordering).exists(_(value))

  def inclusionTest[X >: V](implicit ordering: MinMaxOrdering[X]): Option[X => Boolean]
}

sealed trait HalfOpenSeekRange[+V] extends InequalitySeekRange[V] {
  def bounds: Bounds[V]

  override def mapBounds[P](f: V => P): HalfOpenSeekRange[P]

  def limit[X >: V](implicit ordering: MinMaxOrdering[X]): Option[Bound[X]] =
    boundLimit(boundOrdering(ordering))

  protected def boundLimit[X >: V](implicit ordering: Ordering[Bound[X]]): Option[Bound[X]]

  protected def boundOrdering[X >: V](implicit ordering: MinMaxOrdering[X]): Ordering[Bound[X]]
}

final case class RangeBetween[+V](greaterThan: RangeGreaterThan[V], lessThan: RangeLessThan[V]) extends InequalitySeekRange[V] {
  override def mapBounds[P](f: V => P): RangeBetween[P] = copy(greaterThan = greaterThan.mapBounds(f), lessThan = lessThan.mapBounds(f))

  override def groupBy[K](f: Bound[V] => K): Map[K, InequalitySeekRange[V]] = {
    val greaterThanBounds = greaterThan.bounds.map(Left(_))
    val lessThanBounds = lessThan.bounds.map(Right(_))
    val allBounds = greaterThanBounds ++ lessThanBounds
    val groupedBounds = allBounds.groupBy[Either[Bound[V], Bound[V]], K] {
      case Left(bound) => f(bound)
      case Right(bound) => f(bound)
    }
    groupedBounds.mapValues[InequalitySeekRange[V]] { bounds =>
      InequalitySeekRange.fromPartitionedBounds(bounds.partition(identity))
    }
  }

  override def inclusionTest[X >: V](implicit ordering: MinMaxOrdering[X]): Option[X => Boolean] = {
    (lessThan.inclusionTest[X](ordering), greaterThan.inclusionTest[X](ordering)) match {
      case (_, None) => None
      case (None, _) => None
      case (Some(lessThanTest), Some(greaterThanTest)) => Some((value: X) => lessThanTest(value) && greaterThanTest(value))
    }
  }
}

final case class RangeGreaterThan[+V](bounds: Bounds[V]) extends HalfOpenSeekRange[V] {

  override def mapBounds[P](f: V => P): RangeGreaterThan[P] =
    copy(bounds = bounds.map(_.map(f)))

  override def groupBy[K](f: Bound[V] => K): Map[K, RangeGreaterThan[V]] =
    bounds.groupBy(f).mapValues(bounds => RangeGreaterThan(bounds))

  override def inclusionTest[X >: V](implicit ordering: MinMaxOrdering[X]): Option[X => Boolean] = {
    limit[X].map { bound =>
      val endPoint = bound.endPoint
      (value: X) => {
        if (value == null || endPoint == null) {
          false
        } else {
          val cmp = ordering.ordering.compare(value, endPoint)
          if (bound.isInclusive) cmp >= 0 else cmp > 0
        }
      }
    }
  }

  protected def boundLimit[X >: V](implicit ordering: Ordering[Bound[X]]): Option[Bound[X]] = {
    val limit = bounds.max[Bound[X]](ordering)
    if (limit.endPoint == null) None else Some(limit)
  }

  protected def boundOrdering[X >: V](implicit ordering: MinMaxOrdering[X]): Ordering[Bound[X]] =
    MaxBoundOrdering(ordering.forMax)
}

final case class RangeLessThan[+V](bounds: Bounds[V]) extends HalfOpenSeekRange[V] {

  override def mapBounds[P](f: V => P): RangeLessThan[P] =
    copy(bounds = bounds.map(_.map(f)))

  override def groupBy[K](f: Bound[V] => K): Map[K, RangeLessThan[V]] =
    bounds.groupBy(f).mapValues(bounds => RangeLessThan(bounds))

  override def inclusionTest[X >: V](implicit ordering: MinMaxOrdering[X]): Option[X => Boolean] = {
    limit[X].map { bound =>
      val endPoint = bound.endPoint
      (value: X) => {
        if (value == null || endPoint == null) {
          false
        } else {
          val cmp = ordering.ordering.compare(value, endPoint)
          if (bound.isInclusive) cmp <= 0 else cmp < 0
        }
      }
    }
  }

  protected def boundLimit[X >: V](implicit ordering: Ordering[Bound[X]]): Option[Bound[X]] = {
    val limit = bounds.min[Bound[X]](ordering)
    if (limit.endPoint == null) None else Some(limit)
  }

  protected def boundOrdering[X >: V](implicit ordering: MinMaxOrdering[X]): Ordering[Bound[X]] =
    MinBoundOrdering(ordering.forMin)
}

final case class PrefixRange(prefix: String) extends SeekRange[String]
