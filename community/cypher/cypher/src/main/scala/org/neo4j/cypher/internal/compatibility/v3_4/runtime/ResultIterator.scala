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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.neo4j.cypher.internal.util.v3_4.{CypherException, TaskCloser}
import org.neo4j.values.AnyValue

import scala.collection.immutable

trait ResultIterator extends Iterator[immutable.Map[String, AnyValue]] {
  def toEager: EagerResultIterator
  def wasMaterialized: Boolean
  def close()
}

class EagerResultIterator(result: ResultIterator) extends ResultIterator {
  override val toList = result.toList
  private val inner = toList.iterator

  override def toEager: EagerResultIterator = this

  override def wasMaterialized = true

  override def hasNext = inner.hasNext

  override def next() = inner.next()

  override def close() = result.close()
}

class ClosingIterator(inner: Iterator[collection.Map[String, AnyValue]],
                      closer: TaskCloser,
                      exceptionDecorator: CypherException => CypherException) extends ResultIterator {

  override def toEager = new EagerResultIterator(this)

  override def wasMaterialized = isEmpty

  override def hasNext: Boolean = failIfThrows {
    if (closer.isClosed) false
    else {
      val innerHasNext = inner.hasNext
      if (!innerHasNext) {
        close(success = true)
      }
      innerHasNext
    }
  }

  override def next(): Map[String, AnyValue] = failIfThrows {
    if (closer.isClosed) Iterator.empty.next()

    val value = inner.next()
    value.toMap
  }

  override def close() {
    close(success = true)
  }

  private def close(success: Boolean) = decoratedCypherException({
    closer.close(success)
  })

  private def failIfThrows[U](f: => U): U = decoratedCypherException({
    try {
      f
    } catch {
      case t: Throwable =>
        close(success = false)
        throw t
    }
  })

  private def decoratedCypherException[U](f: => U): U = try {
    f
  } catch {
    case e: CypherException =>
      throw exceptionDecorator(e)
  }
}
