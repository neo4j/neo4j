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
import org.neo4j.cypher.internal.runtime.ClosingIterator.JavaIteratorAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe
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
import org.neo4j.values.storable.LongArray
import org.neo4j.values.storable.Values

/**
 * Slotted variant of [[GroupingAggTable]] when we have only primitive (nodes or relationships) grouping columns.
 */
class SlottedPrimitiveGroupingAggTable(
  slots: SlotConfiguration,
  readGrouping: Array[Int], // Offsets into the long array of the current execution context
  writeGrouping: Array[Int], // Offsets into the long array of the current execution context
  aggregations: Map[Int, AggregationExpression],
  state: QueryState,
  operatorId: Id,
  argumentSize: SlotConfiguration.Size
) extends AggregationTable {

  private[this] var resultMap: HeapTrackingOrderedAppendMap[LongArray, Array[AggregationFunction]] = _

  private[this] val (aggregationOffsets: Array[Int], aggregationExpressions: Array[AggregationExpression]) = {
    val (a, b) = aggregations.unzip
    (a.toArray, b.toArray)
  }
  private[this] val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(operatorId.x)

  private[this] val newAggregators: Function2[LongArray, MemoryTracker, Array[AggregationFunction]] =
    computeNewAggregatorsFunction(aggregationExpressions)

  private def computeGroupingKey(row: CypherRow): LongArray = {
    val keys = new Array[Long](readGrouping.length)
    var i = 0
    while (i < readGrouping.length) {
      keys(i) = row.getLongAt(readGrouping(i))
      i += 1
    }
    Values.longArray(keys)
  }

  private def projectGroupingKey(ctx: CypherRow, key: LongArray): Unit = {
    var i = 0
    while (i < writeGrouping.length) {
      ctx.setLongAt(writeGrouping(i), key.longValue(i))
      i += 1
    }
  }

  private def createResultRow(groupingKey: LongArray, aggregateFunctions: Seq[AggregationFunction]): CypherRow = {
    val row = SlottedRow(slots)
    if (state.initialContext.nonEmpty) {
      row.copyFrom(
        state.initialContext.get,
        Math.min(argumentSize.nLongs, slots.numberOfLongs),
        Math.min(argumentSize.nReferences, slots.numberOfReferences)
      )
    }
    projectGroupingKey(row, groupingKey)
    var i = 0
    while (i < aggregateFunctions.length) {
      row.setRefAt(aggregationOffsets(i), aggregateFunctions(i).result(state))
      i += 1
    }
    row
  }

  override def clear(): Unit = {
    if (resultMap != null) {
      resultMap.close()
    }
    resultMap = HeapTrackingOrderedAppendMap.createOrderedMap[LongArray, Array[AggregationFunction]](memoryTracker)
    state.query.resources.trace(resultMap)
  }

  override def processRow(row: CypherRow): Unit = {
    val groupingValue = computeGroupingKey(row)
    val functions = resultMap.getIfAbsentPutWithMemoryTracker2(groupingValue, newAggregators)
    var i = 0
    while (i < functions.length) {
      functions(i)(row, state)
      i += 1
    }
  }

  override def result(): ClosingIterator[CypherRow] = {
    resultMap.autoClosingEntryIterator.asClosingIterator.map {
      (e: java.util.Map.Entry[LongArray, Array[AggregationFunction]]) => createResultRow(e.getKey, e.getValue)
    }.closing(resultMap)
  }
}

object SlottedPrimitiveGroupingAggTable {

  case class Factory(
    slots: SlotConfiguration,
    readGrouping: Array[Int],
    writeGrouping: Array[Int],
    aggregations: Map[Int, AggregationExpression],
    argumentSize: SlotConfiguration.Size
  ) extends AggregationTableFactory {

    override def table(
      state: QueryState,
      rowFactory: CypherRowFactory,
      operatorId: Id
    ): AggregationPipe.AggregationTable =
      new SlottedPrimitiveGroupingAggTable(
        slots,
        readGrouping,
        writeGrouping,
        aggregations,
        state,
        operatorId,
        argumentSize
      )
  }

}
