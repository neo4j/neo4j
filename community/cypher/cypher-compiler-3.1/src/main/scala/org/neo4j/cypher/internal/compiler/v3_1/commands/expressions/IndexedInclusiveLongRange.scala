/*
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
package org.neo4j.cypher.internal.compiler.v3_1.commands.expressions

import scala.collection.immutable

/**
  * An inclusive long range that is also indexable as long as the total length of the range is less than `Int.MaxValue`
  * @param start beginning of range (inclusive)
  * @param end end of range (inclusive)
  * @param step step between elements of range
  */
case class IndexedInclusiveLongRange(start: Long, end: Long, step: Long) extends immutable.IndexedSeq[Long] {

  private val check: (Long, Long) => Boolean = if (step.signum > 0) _ <= _ else _ >= _

  override def iterator: Iterator[Long] = new Iterator[Long] {
    private var current = start

    override def hasNext: Boolean = check(current, end)

    override def next(): Long = {
      val c = current
      current = current + step
      c
    }
  }

  private var len = -1

  override def length: Int = if (len != -1) len
  else {
    val l = ((end - start) / step) + 1
    if (l > Int.MaxValue) throw new OutOfMemoryError(s"Cannot index an collection of size $l")
    len = l.toInt
    len
  }

  override def apply(idx: Int): Long = if (idx >= length) throw new IndexOutOfBoundsException else start + idx * step
}
