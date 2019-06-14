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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.exceptionHandler.runSafely
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{QueryProfile, RuntimeResult}
import org.neo4j.kernel.impl.query.QuerySubscriber

class PipeExecutionResult(inner: Iterator[_],
                          val fieldNames: Array[String],
                          val state: QueryState,
                          override val queryProfile: QueryProfile,
                          subscriber: QuerySubscriber)
  extends RuntimeResult {

  private var resultRequested = false
  private var demand = 0L
  private var cancelled = false
  private val numberOfFields = fieldNames.length
  private var error: Throwable = _

  override def queryStatistics(): QueryStatistics = state.getStatistics

  override def close(): Unit = {
    state.close()
  }

  override def consumptionState: RuntimeResult.ConsumptionState =
    if (!resultRequested) ConsumptionState.NOT_STARTED
    else if (inner.hasNext) ConsumptionState.HAS_MORE
    else ConsumptionState.EXHAUSTED

  subscriber.onResult(numberOfFields)

  override def request(numberOfRecords: Long): Unit = {
    resultRequested = true
    demand = checkForOverflow(demand + numberOfRecords)
    serveResults()
  }

  override def cancel(): Unit = {
    cancelled = true
  }

  override def await(): Boolean = {
    if (error != null) {
      throw error
    }
    inner.hasNext && !cancelled
  }

  private def serveResults(): Unit = {
    while (inner.hasNext && demand > 0 && !cancelled) {
      runSafely[Unit](inner.next())(t => {
        error = t
        subscriber.onError(t)
        cancel()
      })
      demand -= 1L
    }
    if (!inner.hasNext && error == null) {
      subscriber.onResultCompleted(state.getStatistics)
    }
  }

  private def checkForOverflow(value: Long): Long =
    if (value < 0) Long.MaxValue else value
}
