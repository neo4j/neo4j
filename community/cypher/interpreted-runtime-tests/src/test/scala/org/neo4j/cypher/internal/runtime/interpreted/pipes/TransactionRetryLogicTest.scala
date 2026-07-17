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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.InterpretedRuntimeTestSuite

class TransactionRetryLogicTest extends InterpretedRuntimeTestSuite {

  test("print delay sequence") {
    val retryLogic = new ExponentialBackoffRetryLogic
    println(
      s"Exponential backoff retry logic: multiplier = ${retryLogic.multiplier}, jitterFactor = ${retryLogic.jitterFactor}, initial delay ns = ${retryLogic.initialRetryDelayNanos}, max delay ns = ${retryLogic.maxRetryDelayNanos}, max time ns = ${retryLogic.maxRetryTimeNanos}"
    )
    var state = retryLogic.newRetryState()
    var i = 0
    while (state.shouldRetryAgain()) {
      val waitNs = state.nanosUntilRetry()
      val delay = state.retryDelayNanos
      println(
        s"$i: $delay ns = ${delay.toDouble / 1000000.0d} ms - wait $waitNs ns = ${waitNs.toDouble / 1000000.0d} ms"
      )
      state = state.computeNextRetryState().asInstanceOf[ExponentialBackoffRetryLogic.ExponentialBackoffRetryState]
      i += 1
    }
  }
}
