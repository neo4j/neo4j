/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.procs

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.result.EmptyQuerySubscription
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.memory.OptionalMemoryTracker
import org.neo4j.values.AnyValue

/**
 *Schema result, as produced by a schema read.
 */
case class SchemaReadRuntimeResult(ctx: QueryContext, subscriber: QuerySubscriber, columnNames: Array[String], result: List[Map[String, AnyValue]])
  extends EmptyQuerySubscription(subscriber) with RuntimeResult {

  private var streamPosition = 0;

  override def fieldNames(): Array[String] = columnNames

  override def request(numberOfRecords: Long): Unit = {
    subscriber.onResult(columnNames.length)

    val records = result.slice(streamPosition, Math.min(streamPosition + numberOfRecords, result.size).toInt)
    streamPosition += records.size
    for (record <- records) {
      subscriber.onRecord()
      for (i <- columnNames.indices) {
        subscriber.onField(i, record(columnNames(i)))
      }
      subscriber.onRecordCompleted()
    }
    if (streamPosition == result.size) subscriber.onResultCompleted(queryStatistics())
  }

  override def queryStatistics(): QueryStatistics = ctx.getOptStatistics.getOrElse(QueryStatistics())

  override def totalAllocatedMemory(): Long = OptionalMemoryTracker.ALLOCATIONS_NOT_TRACKED

  override def consumptionState: RuntimeResult.ConsumptionState = {
    if (streamPosition == 0) {
      ConsumptionState.NOT_STARTED
    } else if (streamPosition < result.size) {
      ConsumptionState.HAS_MORE
    } else {
      ConsumptionState.EXHAUSTED
    }
  }

  override def close(): Unit = {}

  override def queryProfile(): QueryProfile = QueryProfile.NONE

  override def await(): Boolean = consumptionState == ConsumptionState.HAS_MORE
}
