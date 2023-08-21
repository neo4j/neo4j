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
package org.neo4j.cypher.internal.runtime.slotted.aggregation

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTableFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CypherRowFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.MemoryTracker

/**
 * Slotted variant of [[NonGroupingAggTable]]
 */
class SlottedNonGroupingAggTable(
  slots: SlotConfiguration,
  aggregations: Map[Int, AggregationExpression],
  state: QueryState,
  operatorId: Id,
  argumentSize: SlotConfiguration.Size
) extends AggregationTable {

  private val (aggregationOffsets: Array[Int], aggregationExpressions: Array[AggregationExpression]) = {
    val (a, b) = aggregations.unzip
    (a.toArray, b.toArray)
  }
  private val aggregationFunctions = new Array[AggregationFunction](aggregationExpressions.length)

  private val scopedMemoryTracker: MemoryTracker =
    state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(operatorId.x).getScopedMemoryTracker

  protected def close(): Unit = {
    scopedMemoryTracker.close()
  }

  override def clear(): Unit = {
    scopedMemoryTracker.reset()
    var i = 0
    while (i < aggregationFunctions.length) {
      aggregationFunctions(i) = aggregationExpressions(i).createAggregationFunction(scopedMemoryTracker)
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
    val row = SlottedRow(slots)
    if (state.initialContext.nonEmpty) {
      row.copyFrom(
        state.initialContext.get,
        Math.min(argumentSize.nLongs, slots.numberOfLongs),
        Math.min(argumentSize.nReferences, slots.numberOfReferences)
      )
    }
    var i = 0
    while (i < aggregationFunctions.length) {
      row.setRefAt(aggregationOffsets(i), aggregationFunctions(i).result(state))
      i += 1
    }
    row
  }
}

object SlottedNonGroupingAggTable {

  case class Factory(
    slots: SlotConfiguration,
    aggregations: Map[Int, AggregationExpression],
    argumentSize: SlotConfiguration.Size
  ) extends AggregationTableFactory {

    override def table(state: QueryState, rowFactory: CypherRowFactory, operatorId: Id): AggregationTable =
      new SlottedNonGroupingAggTable(slots, aggregations, state, operatorId, argumentSize)
  }

}
