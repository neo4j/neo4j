/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cypher.internal.frontend.v2_3.{Bounds, Bound}

/*
  Seek ranges describe intervals. In practice they are used to summarize all inequalities over the
  same node and property (n.prop) during planning, esp. for generating index seek by range plans.

  Seek ranges are used both during planning (in the ast), and at runtime (as a value). To achieve this
  they are generic in the type of the actual limits used by their bounds (type parameter V).
 */
sealed trait SeekRange[+V]

/*
  An inequality seek range can either only have lower bounds (RangeGreaterThan),
  or upper bounds (RangeLessThan),or both (RangeBetween). Bounds can either be inclusive or exclusive
  to differentiate between< and <= or > and >= respectively.  Multiple bounds are needed since
  it is not known at compile time which ast expression is the maximum lower or the minimum upper bound.
 */
object InequalitySeekRange {

  /*
   Construct an inequality seek range given either some lower bounds and optionally some upper bounds - or -
   optionally some lower bounds and some upper bounds.  Such an input may be obtained by partitioning
   inequality expressions.  See usages of this method for examples.
   */
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

  // Test if value falls into this range using the implicitly given ordering
  // (returns None if the value is a 'null' value)
  def includes[X >: V](value: X)(implicit ordering: MinMaxOrdering[X]): Boolean =
    inclusionTest[X](ordering).exists(_(value))

  // Function for testing if a value falls into this range using the implicitly given ordering
  // (returns None if the value is a 'null' value)
  def inclusionTest[X >: V](implicit ordering: MinMaxOrdering[X]): Option[X => Boolean]
}

sealed trait HalfOpenSeekRange[+V] extends InequalitySeekRange[V] {
  def bounds: Bounds[V]

  override def mapBounds[P](f: V => P): HalfOpenSeekRange[P]

  // returns the limit of this half open seek range, i.e.
  // the greatest bound if this is a RangeGreaterThan and
  // the smallest bound if this is a RangeLessThan
  //
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

/*
  PrefixRange is used to describe intervals on string values for prefix search.

  This is practical for two reasons:
  - It directly maps on prefix queries of index implementations
  - It removes the need to construct a proper upper bound value for an interval that
  would describe the prefix search (which can be difficult due to unicode issues)
*/
final case class PrefixRange[T](prefix: T) extends SeekRange[T] {
  def map[X](f: T => X): PrefixRange[X] = copy(f(prefix))

  override def toString: String = prefix.toString
}
