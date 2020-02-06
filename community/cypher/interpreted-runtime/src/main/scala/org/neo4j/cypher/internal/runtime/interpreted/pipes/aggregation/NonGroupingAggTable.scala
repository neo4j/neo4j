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

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.{AggregatingCol, AggregationTable, AggregationTableFactory}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{ExecutionContextFactory, Pipe, QueryState}
import org.neo4j.cypher.internal.util.attribution.Id

/**
  * This table can be used when we have no grouping columns, or there is a provided order for all grouping columns.
  * @param aggregations all aggregation columns
  */
class NonGroupingAggTable(aggregations: Array[AggregatingCol],
                          state: QueryState,
                          executionContextFactory: ExecutionContextFactory,
                          operatorId: Id) extends AggregationTable {
  private val aggregationFunctions = new Array[AggregationFunction](aggregations.length)

  override def clear(): Unit = {
    var i = 0
    while (i < aggregationFunctions.length) {
      aggregationFunctions(i) = aggregations(i).expression.createAggregationFunction(operatorId)
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

  override def result(): Iterator[CypherRow] = {
    Iterator.single(resultRow())
  }

  protected def resultRow(): CypherRow = {
    val row = executionContextFactory.newExecutionContext()
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
    override def table(state: QueryState, executionContextFactory: ExecutionContextFactory, operatorId: Id): AggregationTable =
      new NonGroupingAggTable(aggregations, state, executionContextFactory, operatorId)

    override def registerOwningPipe(pipe: Pipe): Unit = {
      aggregations.foreach(_.expression.registerOwningPipe(pipe))
    }
  }
}
