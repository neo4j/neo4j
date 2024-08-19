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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.AggregationSkippedNull
import org.neo4j.values.AnyValue

/**
 * Base class for aggregation functions. The function is stateful
 * and aggregates by having it's apply method called once for every
 * row that matches the key.
 */
abstract class AggregationFunction {

  private var seenNoValue: Boolean = false

  /**
   * Adds this data to the aggregated total.
   */
  def apply(data: ReadableRow, state: QueryState): Unit

  /**
   * The aggregated result.
   */
  def result(state: QueryState): AnyValue

  protected def onNoValue(state: QueryState): Unit = {
    // seenNoValue is not needed for correctness but since we only
    // need to warn once this will create less deduplication work
    // for the query state
    if (!seenNoValue) {
      state.newRuntimeNotification(AggregationSkippedNull)
      seenNoValue = true
    }
  }
}
