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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.collection.primitive.{PrimitiveIntIterator, PrimitiveLongIterator}
import org.neo4j.cypher.internal.util.v3_4.EntityNotFoundException
import org.neo4j.kernel.api

object JavaConversionSupport {

  def asScala(iterator: PrimitiveIntIterator): Iterator[Int] = new Iterator[Int] {
    def hasNext = iterator.hasNext
    def next() = iterator.next()
  }

  def asScalaENFXSafe(iterator: PrimitiveIntIterator): Iterator[Int] = makeENFXSafe(iterator.hasNext, iterator.next)(identity)

  // Same as mapToScala, but handles concurrency exceptions by swallowing exceptions
  def mapToScalaENFXSafe[T](iterator: PrimitiveLongIterator)(f: Long => T): Iterator[T] = makeENFXSafe(iterator.hasNext, iterator.next)(f)

  private def makeENFXSafe[S,T](hasMore: () => Boolean, more: () => S)(f: S => T): Iterator[T] = new Iterator[T] {
    private var _next: Option[T] = fetchNext()

    // Init
    private def fetchNext(): Option[T] = {
      _next = None
      while (_next.isEmpty && hasMore()) {
        try {
          _next = Some(f(more()))
        } catch {
          case _: api.exceptions.EntityNotFoundException => // IGNORE
          case _: EntityNotFoundException => // IGNORE
        }
      }
      _next
    }

    def hasNext = _next.nonEmpty

    def next() = {
      val r = _next.getOrElse(throw new NoSuchElementException("next on empty result"))
      fetchNext()
      r
    }
  }
}
