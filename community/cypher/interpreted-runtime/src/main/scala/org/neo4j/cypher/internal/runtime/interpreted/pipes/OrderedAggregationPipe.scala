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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.{AggregationTable, AggregationTableFactory}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.{OrderedGroupingAggTable, OrderedNonGroupingAggTable}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

/**
  * Specialization of [[EagerAggregationPipe]] that leverages the order of some or all grouping columns.
  * Will use [[OrderedGroupingAggTable]] if some grouping columns do not have a provided order and
  * [[OrderedNonGroupingAggTable]] if all grouping columns have a provided order.
  */
case class OrderedAggregationPipe(source: Pipe,
                                  tableFactory: OrderedAggregationTableFactory)
                                 (val id: Id = Id.INVALID_ID)
  extends AggregationPipe(source, tableFactory) with OrderedInputPipe {
  override def getReceiver(state: QueryState): OrderedChunkReceiver = tableFactory.table(state, executionContextFactory)
}

trait OrderedAggregationTableFactory extends AggregationTableFactory {
  override def table(state: QueryState, executionContextFactory: ExecutionContextFactory): AggregationTable with OrderedChunkReceiver
}
