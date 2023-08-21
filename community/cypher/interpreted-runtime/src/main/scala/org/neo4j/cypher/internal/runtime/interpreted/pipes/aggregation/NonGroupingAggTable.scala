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

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregatingCol
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTableFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CypherRowFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.MemoryTracker

/**
 * This table can be used when we have no grouping columns, or there is a provided order for all grouping columns.
 *
 * @param aggregations all aggregation columns
 */
class NonGroupingAggTable(
  aggregations: Array[AggregatingCol],
  state: QueryState,
  rowFactory: CypherRowFactory,
  operatorId: Id
) extends AggregationTable {

  private val aggregationFunctions =
    new Array[AggregationFunction](aggregations.length) // We do not track this allocation, but it should be negligable

  private val scopedMemoryTracker: MemoryTracker =
    state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(operatorId.x).getScopedMemoryTracker

  protected def close(): Unit = {
    scopedMemoryTracker.close()
  }

  override def clear(): Unit = {
    scopedMemoryTracker.reset()
    var i = 0
    while (i < aggregationFunctions.length) {
      aggregationFunctions(i) = aggregations(i).expression.createAggregationFunction(scopedMemoryTracker)
      i += 1
    }
  }

  override def processRow(row: CypherRow): Unit = {
    var i = 0
    while (i < aggregationFunctions.length) {
      aggregationFunctions(i)(row, state)
      i += 1
    }
  }

  override def result(): ClosingIterator[CypherRow] = {
    val row = resultRow()
    scopedMemoryTracker.close()
    ClosingIterator.single(row)
  }

  protected def resultRow(): CypherRow = {
    val row = state.newRow(rowFactory)
    var i = 0
    while (i < aggregationFunctions.length) {
      row.set(aggregations(i).key, aggregationFunctions(i).result(state))
      i += 1
    }
    row
  }
}

object NonGroupingAggTable {

  case class Factory(aggregations: Array[AggregatingCol]) extends AggregationTableFactory {

    override def table(state: QueryState, rowFactory: CypherRowFactory, operatorId: Id): AggregationTable =
      new NonGroupingAggTable(aggregations, state, rowFactory, operatorId)
  }
}
