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

import org.eclipse.collections.api.block.function.Function2
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTableFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.computeNewAggregatorsFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CypherRowFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.HeapTrackingOrderedAppendMap
import org.neo4j.memory.MemoryTracker

/**
 * Slotted variant of [[GroupingAggTable]]
 */
class SlottedGroupingAggTable(
  slots: SlotConfiguration,
  groupingColumns: GroupingExpression,
  aggregations: Map[Int, AggregationExpression],
  state: QueryState,
  operatorId: Id,
  argumentSize: SlotConfiguration.Size
) extends AggregationTable {

  private[this] var resultMap: HeapTrackingOrderedAppendMap[groupingColumns.KeyType, Array[AggregationFunction]] = _

  private[this] val (aggregationOffsets: Array[Int], aggregationExpressions: Array[AggregationExpression]) = {
    val (a, b) = aggregations.unzip
    (a.toArray, b.toArray)
  }
  private[this] val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(operatorId.x)

  private[this] val newAggregators: Function2[groupingColumns.KeyType, MemoryTracker, Array[AggregationFunction]] =
    computeNewAggregatorsFunction(aggregationExpressions)

  protected def close(): Unit = {
    if (resultMap != null) {
      resultMap.close()
    }
  }

  override def clear(): Unit = {
    close()
    resultMap =
      HeapTrackingOrderedAppendMap.createOrderedMap[groupingColumns.KeyType, Array[AggregationFunction]](memoryTracker)
    state.query.resources.trace(resultMap)
  }

  override def processRow(row: CypherRow): Unit = {
    val groupingValue = groupingColumns.computeGroupingKey(row, state)
    val functions = resultMap.getIfAbsentPutWithMemoryTracker2(groupingValue, newAggregators)
    var i = 0
    while (i < functions.length) {
      functions(i)(row, state)
      i += 1
    }
  }

  override def result(): ClosingIterator[CypherRow] = {
    val innerIterator = resultMap.autoClosingEntryIterator()
    new ClosingIterator[CypherRow] {
      override protected[this] def closeMore(): Unit = resultMap.close()

      override def innerHasNext: Boolean = innerIterator.hasNext

      override def next(): CypherRow = {
        val entry = innerIterator.next() // NOTE: This entry is transient and only valid until we call next() again
        val unorderedGroupingValue = entry.getKey
        val aggregateFunctions = entry.getValue
        val row = SlottedRow(slots)
        if (state.initialContext.nonEmpty) {
          row.copyFrom(
            state.initialContext.get,
            Math.min(argumentSize.nLongs, slots.numberOfLongs),
            Math.min(argumentSize.nReferences, slots.numberOfReferences)
          )
        }
        groupingColumns.project(row, unorderedGroupingValue)
        var i = 0
        while (i < aggregateFunctions.length) {
          row.setRefAt(aggregationOffsets(i), aggregateFunctions(i).result(state))
          i += 1
        }
        row
      }
    }
  }
}

object SlottedGroupingAggTable {

  case class Factory(
    slots: SlotConfiguration,
    groupingColumns: GroupingExpression,
    aggregations: Map[Int, AggregationExpression],
    argumentSize: SlotConfiguration.Size
  ) extends AggregationTableFactory {

    override def table(state: QueryState, rowFactory: CypherRowFactory, operatorId: Id): AggregationTable =
      new SlottedGroupingAggTable(slots, groupingColumns, aggregations, state, operatorId, argumentSize)
  }

}
