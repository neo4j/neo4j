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

import org.eclipse.collections.api.block.function.Function2
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregatingCol
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTableFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.computeNewAggregatorsFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CypherRowFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DistinctPipe.GroupingCol
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.HeapTrackingOrderedAppendMap
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue

/**
 * This table must be used when we have grouping columns, and there is no provided order for at least one grouping column.
 *
 * @param groupingColumns  all grouping columns
 * @param groupingFunction a precomputed function to calculate the grouping key of a row
 * @param aggregations     all aggregation columns
 */
class GroupingAggTable(
  groupingColumns: Array[GroupingCol],
  groupingFunction: (CypherRow, QueryState) => AnyValue,
  aggregations: Array[AggregatingCol],
  state: QueryState,
  rowFactory: CypherRowFactory,
  operatorId: Id
) extends AggregationTable {

  private[this] var resultMap: HeapTrackingOrderedAppendMap[AnyValue, Array[AggregationFunction]] = _

  private[this] val addKeys: (CypherRow, AnyValue) => Unit =
    AggregationPipe.computeAddKeysToResultRowFunction(groupingColumns)
  private[this] val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(operatorId.x)

  private[this] val newAggregators: Function2[AnyValue, MemoryTracker, Array[AggregationFunction]] =
    computeNewAggregatorsFunction(aggregations.map(_.expression))

  protected def close(): Unit = {
    if (resultMap != null) {
      resultMap.close()
    }
  }

  override def clear(): Unit = {
    close()
    resultMap = HeapTrackingOrderedAppendMap.createOrderedMap[AnyValue, Array[AggregationFunction]](memoryTracker)
    state.query.resources.trace(resultMap)
  }

  override def processRow(row: CypherRow): Unit = {
    val groupingValue: AnyValue = groupingFunction(row, state)
    val aggregationFunctions = resultMap.getIfAbsentPutWithMemoryTracker2(groupingValue, newAggregators)
    var i = 0
    while (i < aggregationFunctions.length) {
      aggregationFunctions(i)(row, state)
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
        val row = state.newRow(rowFactory)
        addKeys(row, unorderedGroupingValue)
        var i = 0
        while (i < aggregateFunctions.length) {
          row.set(aggregations(i).key, aggregateFunctions(i).result(state))
          i += 1
        }
        row

      }
    }
  }

}

object GroupingAggTable {

  case class Factory(
    groupingColumns: Array[GroupingCol],
    groupingFunction: (CypherRow, QueryState) => AnyValue,
    aggregations: Array[AggregatingCol]
  ) extends AggregationTableFactory {

    override def table(state: QueryState, rowFactory: CypherRowFactory, operatorId: Id): AggregationTable =
      new GroupingAggTable(groupingColumns, groupingFunction, aggregations, state, rowFactory, operatorId)
  }

}
