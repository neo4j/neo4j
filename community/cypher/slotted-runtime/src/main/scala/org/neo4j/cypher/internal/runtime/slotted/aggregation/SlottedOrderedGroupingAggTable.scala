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
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CypherRowFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedAggregationTableFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedChunkReceiver
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Slotted variant of [[OrderedGroupingAggTable]]
 */
class SlottedOrderedGroupingAggTable(
  slots: SlotConfiguration,
  orderedGroupingColumns: GroupingExpression,
  unorderedGroupingColumns: GroupingExpression,
  aggregations: Map[Int, AggregationExpression],
  state: QueryState,
  operatorId: Id,
  argumentSize: SlotConfiguration.Size
) extends SlottedGroupingAggTable(slots, unorderedGroupingColumns, aggregations, state, operatorId, argumentSize)
    with OrderedChunkReceiver {

  private var currentGroupKey: orderedGroupingColumns.KeyType = _

  override def close(): Unit = {
    currentGroupKey = null.asInstanceOf[orderedGroupingColumns.KeyType]
    super.close()
  }

  override def clear(): Unit = {
    currentGroupKey = null.asInstanceOf[orderedGroupingColumns.KeyType]
    super.clear()
  }

  override def isSameChunk(first: CypherRow, current: CypherRow): Boolean = {
    if (currentGroupKey == null) {
      currentGroupKey = orderedGroupingColumns.computeGroupingKey(first, state)
    }
    current.eq(first) || currentGroupKey == orderedGroupingColumns.computeGroupingKey(current, state)
  }

  override def result(): ClosingIterator[CypherRow] = {
    super.result().map { row =>
      orderedGroupingColumns.project(row, currentGroupKey)
      row
    }
  }

  override def processNextChunk: Boolean = true
}

object SlottedOrderedGroupingAggTable {

  case class Factory(
    slots: SlotConfiguration,
    orderedGroupingColumns: GroupingExpression,
    unorderedGroupingColumns: GroupingExpression,
    aggregations: Map[Int, AggregationExpression],
    argumentSize: SlotConfiguration.Size
  ) extends OrderedAggregationTableFactory {

    override def table(
      state: QueryState,
      rowFactory: CypherRowFactory,
      operatorId: Id
    ): AggregationTable with OrderedChunkReceiver =
      new SlottedOrderedGroupingAggTable(
        slots,
        orderedGroupingColumns,
        unorderedGroupingColumns,
        aggregations,
        state,
        operatorId,
        argumentSize
      )
  }
}
