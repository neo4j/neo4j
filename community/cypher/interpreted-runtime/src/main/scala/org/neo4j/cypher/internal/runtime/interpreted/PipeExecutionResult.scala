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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionCommittedCounterIterator.wrap
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.kernel.impl.query.QuerySubscriber

import java.util

class PipeExecutionResult(
  pipe: Pipe,
  val fieldNames: Array[String],
  val state: QueryState,
  override val queryProfile: QueryProfile,
  subscriber: QuerySubscriber,
  startsTransactions: Boolean
) extends RuntimeResult {

  private var demand = 0L
  private var cancelled = false
  private var inner: ClosingIterator[_] = _
  private val numberOfFields = fieldNames.length
  private var nonEmptyResult = false

  override def hasServedRows: Boolean = nonEmptyResult

  override def getErrorOrNull: Throwable = null

  override def queryStatistics(): QueryStatistics = state.getStatistics

  override def heapHighWaterMark: Long = state.queryMemoryTracker.heapHighWaterMark

  override def close(): Unit = {
    state.close()
    if (inner != null) {
      inner.close()
    }
  }

  override def consumptionState: RuntimeResult.ConsumptionState =
    if (inner == null) ConsumptionState.NOT_STARTED
    else if (inner.hasNext) ConsumptionState.HAS_MORE
    else ConsumptionState.EXHAUSTED

  subscriber.onResult(numberOfFields)

  override def request(numberOfRecords: Long): Unit = {
    if (!cancelled) {
      if (inner == null) {
        if (startsTransactions) {
          inner = wrap(() => pipe.createResults(state), state)
        } else {
          inner = pipe.createResults(state)
        }
      }
      demand = checkForOverflow(demand + numberOfRecords)
      serveResults()
    }
  }

  override def cancel(): Unit = {
    cancelled = true
  }

  override def await(): Boolean = {
    inner == null || (inner.hasNext && !cancelled)
  }

  override def notifications(): util.Set[InternalNotification] = state.notifications()

  private def serveResults(): Unit = {
    while (inner.hasNext && demand > 0 && !cancelled) {
      nonEmptyResult = true
      inner.next()
      demand -= 1L
    }
    if (!inner.hasNext) {
      subscriber.onResultCompleted(state.getStatistics)
    }
  }

  private def checkForOverflow(value: Long): Long =
    if (value < 0) Long.MaxValue else value
}
