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
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

class ReactiveIterator(inner: Iterator[Array[AnyValue]], result: RuntimeResult, indexMap: Int => Int = identity) extends Iterator[Array[AnyValue]] {
  private var demand = 0L
  private var served = 0L
  private var cancelled = false

  def addDemand(numberOfRecords: Long): Unit = {
    val newDemand = demand + numberOfRecords
    if (newDemand < 0) {
      demand = Long.MaxValue
    } else {
      demand = newDemand
    }
  }

  def cancel(): Unit = {
    cancelled = true
  }

  override def hasNext: Boolean = inner.hasNext && served < demand

  override def next(): Array[AnyValue] = {
    served += 1L
    inner.next()
  }

  def await(subscriber: QuerySubscriber): Boolean = {
    val numberOfFields = result.fieldNames().length
    subscriber.onResult(numberOfFields)
    while (hasNext) {
      val values = next()
      subscriber.onRecord()
      var i = 0
      while (i < numberOfFields) {
        subscriber.onField(i, values(indexMap(i)))
        i += 1
      }
      subscriber.onRecordCompleted()
    }

    if (!inner.hasNext) {
      subscriber.onResultCompleted(result.queryStatistics())
    }

    inner.hasNext && !cancelled
  }
}
