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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.{AggregatingCol, AggregationTable, AggregationTableFactory}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DistinctPipe.GroupingCol
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{AggregationPipe, ExecutionContextFactory, Pipe, QueryState}
import org.neo4j.values.AnyValue

import scala.collection.JavaConverters._

/**
  * This table must be used when we have grouping columns, and there is no provided order for at least one grouping column.
  *
  * @param groupingColumns  all grouping columns
  * @param groupingFunction a precomputed function to calculate the grouping key of a row
  * @param aggregations     all aggregation columns
  */
class GroupingAggTable(groupingColumns: Array[GroupingCol],
                       groupingFunction: (ExecutionContext, QueryState) => AnyValue,
                       aggregations: Array[AggregatingCol],
                       state: QueryState,
                       executionContextFactory: ExecutionContextFactory) extends AggregationTable {

  protected var resultMap: java.util.LinkedHashMap[AnyValue, Array[AggregationFunction]] = _
  protected val addKeys: (ExecutionContext, AnyValue) => Unit = AggregationPipe.computeAddKeysToResultRowFunction(groupingColumns)

  override def clear(): Unit = {
    resultMap = new java.util.LinkedHashMap[AnyValue, Array[AggregationFunction]]()
  }

  override def processRow(row: ExecutionContext): Unit = {
    val groupingValue: AnyValue = groupingFunction(row, state)
    val aggregationFunctions = resultMap.computeIfAbsent(groupingValue, _ => {
      state.memoryTracker.allocated(groupingValue)
      val functions = new Array[AggregationFunction](aggregations.length)
      var i = 0
      while (i < aggregations.length) {
        functions(i) = aggregations(i).expression.createAggregationFunction
        i += 1
      }
      functions
    })
    var i = 0
    while (i < aggregationFunctions.length) {
      aggregationFunctions(i)(row, state)
      i += 1
    }
  }

  override def result(): Iterator[ExecutionContext] = {
    val innerIterator = resultMap.entrySet().iterator()
    new Iterator[ExecutionContext] {
      override def hasNext: Boolean = innerIterator.hasNext

      override def next(): ExecutionContext = {
        val entry = innerIterator.next()
        val unorderedGroupingValue = entry.getKey
        val aggregateFunctions = entry.getValue
        val row = executionContextFactory.newExecutionContext()
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

  case class Factory(groupingColumns: Array[GroupingCol],
                     groupingFunction: (ExecutionContext, QueryState) => AnyValue,
                     aggregations: Array[AggregatingCol]) extends AggregationTableFactory {
    override def table(state: QueryState, executionContextFactory: ExecutionContextFactory): AggregationTable =
      new GroupingAggTable(groupingColumns, groupingFunction, aggregations, state, executionContextFactory)

    override def registerOwningPipe(pipe: Pipe): Unit = {
      aggregations.foreach(_.expression.registerOwningPipe(pipe))
      groupingColumns.foreach(_.expression.registerOwningPipe(pipe))
    }
  }

}
