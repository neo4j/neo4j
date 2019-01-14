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
package org.neo4j.cypher.internal.runtime

abstract class PrefetchingIterator[T] extends Iterator[T] {
  private var buffer: Option[T] = _

  def produceNext(): Option[T]

  override def hasNext: Boolean = {
    if (buffer == null)
      pullNextElementFromSource()
    buffer.nonEmpty
  }

  override def next(): T = {
    if (!hasNext)
      throw new NoSuchElementException("next on empty iterator")
    else {
      val current = buffer.get
      pullNextElementFromSource()
      current
    }
  }

  private def pullNextElementFromSource(): Unit = {
    buffer = produceNext()
  }
}
