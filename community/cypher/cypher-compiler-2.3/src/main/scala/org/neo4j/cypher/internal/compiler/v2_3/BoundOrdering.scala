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

import org.neo4j.cypher.internal.frontend.v2_3.Bound

// Tested by SeekRangeTest
final case class MinBoundOrdering[T](inner: Ordering[T]) extends Ordering[Bound[T]] {
  override def compare(x: Bound[T], y: Bound[T]): Int = {
    val cmp = inner.compare(x.endPoint, y.endPoint)
    if (cmp == 0)
      Ordering.Boolean.compare(x.isInclusive, y.isInclusive)
    else
      cmp
  }
}

// Tested by SeekRangeTest
final case class MaxBoundOrdering[T](inner: Ordering[T]) extends Ordering[Bound[T]] {
  override def compare(x: Bound[T], y: Bound[T]): Int = {
    val cmp = inner.compare(x.endPoint, y.endPoint)
    if (cmp == 0)
      Ordering.Boolean.compare(y.isInclusive, x.isInclusive)
    else
      cmp
  }
}

