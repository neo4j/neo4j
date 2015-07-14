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

// Tested by SeekRangeTest
sealed trait BoundOrdering[T] extends Ordering[Bound[T]] {
  def inner: Ordering[T]

  override def compare(x: Bound[T], y: Bound[T]): Int = {
    val lhs = x.endPoint
    val rhs = y.endPoint

    val cmp =
      if (lhs == null) {
        if (rhs == null) 0 else flip(-1)
      } else if (rhs == null) {
        flip(+1)
      } else {
        inner.compare(lhs, rhs)
      }

    if (cmp == 0)
      flip(Ordering.Boolean.compare(x.isInclusive, y.isInclusive))
    else
      cmp
  }

  def flip(cmp: Int): Int
}

final case class MinBoundOrdering[T](inner: Ordering[T]) extends BoundOrdering[T] {
  def flip(cmp: Int): Int = cmp
}

final case class MaxBoundOrdering[T](inner: Ordering[T]) extends BoundOrdering[T] {
  def flip(cmp: Int): Int = -cmp
}

