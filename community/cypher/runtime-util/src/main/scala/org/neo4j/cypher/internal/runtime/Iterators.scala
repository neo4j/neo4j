/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime

// We have equivalent classes in Java in org.neo4j.internal.helpers.collection.Iterators
// but using these in Scala requires conversion back-and forth with scala.collection.JavaConverters
object Iterators {
  /**
   * Create an iterator that automatically closes the given resource when exhausted.
   */
  def resourceClosingIterator[T](iterator: Iterator[T], resource: AutoCloseable): Iterator[T] = {
    new ResourceClosingIterator(iterator, resource)
  }
}

/**
 * An iterator that calls close() when the inner iterator is exhausted.
 *
 * NOTE: Since this is not a prefetching iterator, you have to iterate in a standard way, calling hasNext for every step, for this to work as intended.
 */
abstract class AutoClosingIterator[T](private[this] val inner: Iterator[T]) extends Iterator[T] {

  def close(): Unit

  override def hasNext: Boolean = {
    val _hasNext = inner.hasNext
    if (!_hasNext) {
      close();
    }
    _hasNext
  }

  override def next(): T = {
    inner.next()
  }
}

private final class ResourceClosingIterator[T](iterator: Iterator[T],
                                               private[this] val resource: AutoCloseable)
  extends AutoClosingIterator[T](iterator) {

  override def close(): Unit = {
    resource.close()
  }
}
