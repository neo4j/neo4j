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
package org.neo4j.cypher.internal.runtime

object Counter {
  def apply(initialCount: Long = 0) = new Counter(initialCount)
}

class Counter(initialCount: Long) {

  self =>

  private var _count: Long = initialCount

  def counted = _count

  def reset(newValue: Long = 0) = {
    _count = newValue
    self
  }

  def +=(amount: Long): Long = {
    _count += amount
    _count
  }

  def -=(amount: Long) = self += (-amount)

  def values = map(identity)

  def map[T](f: Long => T) = new CountingIterator[T] {
    override def hasNext = true
    override def next() = f(self += 1)
  }

  def track[T](tracked: Iterator[T]) = new CountingIterator[T] {
    override def hasNext = tracked.hasNext
    override def next() = {
      val result = tracked.next()
      self += 1
      result
    }
  }

  trait CountingIterator[T] extends Iterator[T] {
    inner =>

    def counted = self.counted

    def tick(): Unit = next()

    def limit(maxCount: Long)(thunk: Long => T) = new CountingIterator[T] {
      override def hasNext = inner.hasNext
      override def next() = if (counted >= maxCount) thunk(self += 1) else inner.next()
    }
  }
}
