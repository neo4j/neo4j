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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.SlotMappings
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.SlotMapper
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.SlotMappers
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection
import org.neo4j.kernel.impl.util.collection.ProbeTable
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE

case class ValueHashJoinSlottedPipe(
  leftSide: Expression,
  rightSide: Expression,
  left: Pipe,
  right: Pipe,
  slots: SlotConfiguration,
  rhsSlotMappings: SlotMappings
)(val id: Id = Id.INVALID_ID)
    extends AbstractHashJoinPipe[AnyValue](left, right) {

  private val rhsMappers: Array[SlotMapper] = SlotMappers(rhsSlotMappings)

  override def probeInput(
    rhsIterator: ClosingIterator[CypherRow],
    state: QueryState,
    table: ProbeTable[AnyValue, CypherRow]
  ): ClosingIterator[SlottedRow] = {
    val result = for {
      rhs <- rhsIterator
      joinKey <- computeKey(rhs, rightSide, state)
      lhs <- ClosingIterator.asClosingIterator(table.get(joinKey))
    } yield {
      val newRow = SlottedRow(slots)
      newRow.copyAllFrom(lhs)
      NodeHashJoinSlottedPipe.copyDataFromRow(rhsMappers, newRow, rhs, state.query)
      newRow
    }
    result.closing(table)
  }

  override def buildProbeTable(
    input: ClosingIterator[CypherRow],
    queryState: QueryState
  ): collection.ProbeTable[AnyValue, CypherRow] = {
    val table = collection.ProbeTable.createProbeTable[AnyValue, CypherRow](
      queryState.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    )

    for {
      context <- input
      joinKey <- computeKey(context, leftSide, queryState)
    } {
      context.compact()
      table.put(joinKey, context)
    }

    table
  }

  private def computeKey(
    context: CypherRow,
    keyColumns: Expression,
    queryState: QueryState
  ): ClosingIterator[AnyValue] = {
    val value = keyColumns.apply(context, queryState)
    if (value eq NO_VALUE) {
      ClosingIterator.empty
    } else {
      ClosingIterator.single(value)
    }
  }
}
