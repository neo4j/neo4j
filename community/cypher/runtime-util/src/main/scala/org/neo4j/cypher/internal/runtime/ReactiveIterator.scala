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

import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.kernel.impl.query.{QuerySubscriber, QuerySubscription}
import org.neo4j.values.AnyValue

class ReactiveIterator(inner: Iterator[Array[AnyValue]], result: RuntimeResult, subscriber: QuerySubscriber, indexMapping: Array[Int] = null) extends QuerySubscription {
  private var demand = 0L
  private var served = 0L
  private var cancelled = false
  private val numberOfFields = result.fieldNames().length

  subscriber.onResult(numberOfFields)

  override def request(numberOfRecords: Long): Unit = {
    demand = checkForOverflow(demand + numberOfRecords)
    serveResults()

  }

  override def cancel(): Unit = {
    cancelled = true
  }

   private def next(): Array[AnyValue] = {
    served += 1L
    inner.next()
  }

  override def await(): Boolean = {
    inner.hasNext && !cancelled
  }

  private def serveResults(): Unit = {
    while (inner.hasNext && served < demand && !cancelled) {
      val values = next()
      subscriber.onRecord()
      var i = 0
      while (i < numberOfFields) {
        subscriber.onField(i, values(mapIndex(i)))
        i += 1
      }
      subscriber.onRecordCompleted()
    }

    if (!inner.hasNext) {
      subscriber.onResultCompleted(result.queryStatistics())
    }
  }

  private def checkForOverflow(value: Long) =
    if (value < 0) Long.MaxValue else value

  private def mapIndex(index: Int) = if (indexMapping == null) index else indexMapping(index)
}
