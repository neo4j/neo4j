/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{AggregationPipe, DistinctPipe, ExecutionContextFactory, OrderedAggregationTableFactory, OrderedChunkReceiver, Pipe, QueryState}
import org.neo4j.values.AnyValue

/**
  * Specialization of [[NonGroupingAggTable]] where we have only grouping columns with provided order.
  *
  * The ordered columns are used to determine chunks for which to aggregate. No Hash Map is needed.
  *
  * @param orderedGroupingFunction a precomputed function to calculate the grouping key
  * @param orderedGroupingColumns all grouping columns
  * @param aggregations all aggregation columns
  */
class OrderedNonGroupingAggTable(orderedGroupingFunction: (ExecutionContext, QueryState) => AnyValue,
                                 orderedGroupingColumns: Array[DistinctPipe.GroupingCol],
                                 aggregations: Array[AggregationPipe.AggregatingCol],
                                 state: QueryState,
                                 executionContextFactory: ExecutionContextFactory)
  extends NonGroupingAggTable(aggregations, state, executionContextFactory) with OrderedChunkReceiver {

  private var currentGroupKey: AnyValue = _

  override def clear(): Unit = {
    currentGroupKey = null
    super.clear()
  }

  override def isSameChunk(first: ExecutionContext, current: ExecutionContext): Boolean = {
    if (currentGroupKey == null) {
      currentGroupKey = orderedGroupingFunction(first, state)
    }
    current.eq(first) || currentGroupKey == orderedGroupingFunction(current, state)
  }

  // This is the result of one chunk, not the whole result
  override def result(): Iterator[ExecutionContext] = {
    val row = resultRow()
    AggregationPipe.computeAddKeysToResultRowFunction(orderedGroupingColumns)(row, currentGroupKey)
    Iterator.single(row)
  }

  override def processNextChunk: Boolean = true
}

object OrderedNonGroupingAggTable {
  case class Factory(orderedGroupingFunction: (ExecutionContext, QueryState) => AnyValue,
                     orderedGroupingColumns: Array[DistinctPipe.GroupingCol],
                     aggregations: Array[AggregationPipe.AggregatingCol]) extends OrderedAggregationTableFactory {
    override def table(state: QueryState, executionContextFactory: ExecutionContextFactory): AggregationTable with OrderedChunkReceiver =
      new OrderedNonGroupingAggTable(orderedGroupingFunction, orderedGroupingColumns, aggregations, state, executionContextFactory)

    override def registerOwningPipe(pipe: Pipe): Unit = {
      aggregations.foreach(_.expression.registerOwningPipe(pipe))
      orderedGroupingColumns.foreach(_.expression.registerOwningPipe(pipe))
    }
  }
}
