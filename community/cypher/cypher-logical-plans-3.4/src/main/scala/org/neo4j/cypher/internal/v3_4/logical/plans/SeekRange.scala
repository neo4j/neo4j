/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_4.logical.plans

/*
  Seek ranges describe intervals. In practice they are used to summarize all inequalities over the
  same node and property (n.prop) during planning, esp. for generating index seek by range plans.
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
}

final case class RangeGreaterThan[+V](bounds: Bounds[V]) extends HalfOpenSeekRange[V] {

  override def mapBounds[P](f: V => P): RangeGreaterThan[P] =
    copy(bounds = bounds.map(_.map(f)))

  override def groupBy[K](f: Bound[V] => K): Map[K, RangeGreaterThan[V]] =
    bounds.groupBy(f).mapValues(bounds => RangeGreaterThan(bounds))

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

  override def toString: String = s"STARTS WITH ${if(prefix == null) "null" else prefix.toString}"
}

final case class PointDistanceRange[T](point: T, distance: T, inclusive: Boolean) extends SeekRange[T] {
  def map[X](f: T => X): PointDistanceRange[X] = copy(f(point), f(distance), inclusive)
}

final case class MinBoundOrdering[T](inner: Ordering[T]) extends Ordering[Bound[T]] {
  override def compare(x: Bound[T], y: Bound[T]): Int = {
    val cmp = inner.compare(x.endPoint, y.endPoint)
    if (cmp == 0)
      Ordering.Boolean.compare(x.isInclusive, y.isInclusive)
    else
      cmp
  }
}

final case class MaxBoundOrdering[T](inner: Ordering[T]) extends Ordering[Bound[T]] {
  override def compare(x: Bound[T], y: Bound[T]): Int = {
    val cmp = inner.compare(x.endPoint, y.endPoint)
    if (cmp == 0)
      Ordering.Boolean.compare(y.isInclusive, x.isInclusive)
    else
      cmp
  }
}

case class MinMaxOrdering[T](ordering: Ordering[T]) {

  import MinMaxOrdering._

  val forMin = ordering.withNullsFirst
  val forMax = ordering.withNullsLast
}

object MinMaxOrdering {

  implicit class NullOrdering[T](ordering: Ordering[T]) {
    def withNullsFirst = new Ordering[T] {
      override def compare(x: T, y: T): Int = {
        if (x == null) {
          if (y == null) 0 else -1
        } else if (y == null) {
          +1
        } else {
          ordering.compare(x, y)
        }
      }
    }

    def withNullsLast = new Ordering[T] {
      override def compare(x: T, y: T): Int = {
        if (x == null) {
          if (y == null) 0 else +1
        } else if (y == null) {
          -1
        } else {
          ordering.compare(x, y)
        }
      }
    }
  }

}
