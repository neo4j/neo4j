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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.neo4j.cypher.internal.util.v3_4.{CypherException, TaskCloser}
import org.neo4j.cypher.result.QueryResult
import org.neo4j.values.AnyValue

trait ResultIterator extends Iterator[collection.Map[String, AnyValue]] {
  def toEager: EagerResultIterator
  def wasMaterialized: Boolean
  def close()
  def recordIterator: Option[Iterator[QueryResult.Record]] = None
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

  override def hasNext: Boolean = {
    if (closer.isClosed) false
    else {
      try {
        val innerHasNext = inner.hasNext
        if (!innerHasNext) {
          close(success = true)
        }
        innerHasNext
      } catch {
        case t: Throwable => safeClose(t)
      }
    }
  }

  override def next(): collection.Map[String, AnyValue] = {
    if (closer.isClosed) Iterator.empty.next()
    try {
      inner.next()
    } catch {
      case t: Throwable => safeClose(t)
    }
  }

  private def safeClose(t: Throwable) = {
    try {
      close(success = false)
    } catch {
      case thrownDuringClose: Throwable =>
        try {
          t.addSuppressed(thrownDuringClose)
        } catch {
          case _: Throwable => // Ignore
        }
    }
    throw t
  }

  override def close() {
    close(success = true)
  }

  private def close(success: Boolean) = {
    try {
      closer.close(success)
    } catch {
      case e: CypherException =>
        throw exceptionDecorator(e)
    }
  }
}

class ClosingQueryResultRecordIterator(inner: Iterator[collection.Map[String, AnyValue]],
                                       closer: TaskCloser,
                                       exceptionDecorator: CypherException => CypherException)
  extends ClosingIterator(inner, closer, exceptionDecorator) {

  override def recordIterator =
    Some(this.asInstanceOf[Iterator[QueryResult.Record]])
}
