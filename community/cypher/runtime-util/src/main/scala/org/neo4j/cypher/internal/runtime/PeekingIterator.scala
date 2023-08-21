/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.macros.AssertMacros

/**
 * Iterator that adds a `peek` method to inspect the next element without removing it from the Iterator.
 *
 * @param inner the wrapped iterator. Must never return `null` on a call to `next`. Undefined behavior otherwise.
 */
class PeekingIterator[T](inner: ClosingIterator[T]) extends ClosingIterator[T] {
  private var buffer: T = _

  override protected[this] def closeMore(): Unit = inner.close()

  override protected[this] def innerHasNext: Boolean = {
    buffer != null || inner.hasNext
  }

  override def next(): T = {
    if (!hasNext) {
      throw new NoSuchElementException("next on empty iterator")
    }
    if (buffer == null) {
      inner.next()
    } else {
      val value = buffer
      buffer = null.asInstanceOf[T]
      value
    }
  }

  def peek(): T = {
    if (!hasNext) {
      throw new NoSuchElementException("peek on empty iterator")
    }
    if (buffer == null) {
      val t = inner.next()
      AssertMacros.checkOnlyWhenAssertionsAreEnabled(
        t != null,
        "Inner of PeekingIterator returned `null` on `next()` call."
      )
      buffer = t
      t
    } else {
      buffer
    }
  }
}
