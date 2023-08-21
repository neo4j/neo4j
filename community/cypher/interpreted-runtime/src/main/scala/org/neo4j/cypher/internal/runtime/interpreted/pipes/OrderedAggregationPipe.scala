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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe.AggregationTableFactory
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Specialization of [[EagerAggregationPipe]] that leverages the order of some or all grouping columns.
 * Will use [[OrderedGroupingAggTable]] if some grouping columns do not have a provided order and
 * [[OrderedNonGroupingAggTable]] if all grouping columns have a provided order.
 */
case class OrderedAggregationPipe(source: Pipe, tableFactory: OrderedAggregationTableFactory)(val id: Id =
  Id.INVALID_ID)
    extends AggregationPipe(source) with OrderedInputPipe {
  override def getReceiver(state: QueryState): OrderedChunkReceiver = tableFactory.table(state, rowFactory, id)
}

trait OrderedAggregationTableFactory extends AggregationTableFactory {

  override def table(
    state: QueryState,
    rowFactory: CypherRowFactory,
    operatorId: Id
  ): AggregationTable with OrderedChunkReceiver
}
