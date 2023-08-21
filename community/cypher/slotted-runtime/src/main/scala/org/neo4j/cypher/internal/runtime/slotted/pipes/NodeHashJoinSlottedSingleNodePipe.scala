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
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.SlotMappings
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.SingleKeyOffset
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.SlotMapper
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.SlotMappers
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.copyDataFromRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.LongProbeTable

import java.util

case class NodeHashJoinSlottedSingleNodePipe(
  lhsKeyOffset: SingleKeyOffset,
  rhsKeyOffset: SingleKeyOffset,
  left: Pipe,
  right: Pipe,
  slots: SlotConfiguration,
  rhsSlotMappings: SlotMappings
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(left) {

  private val rhsMappers: Array[SlotMapper] = SlotMappers(rhsSlotMappings)
  private val lhsOffset: Int = lhsKeyOffset.offset
  private val lhsIsReference: Boolean = lhsKeyOffset.isReference
  private val rhsOffset: Int = rhsKeyOffset.offset
  private val rhsIsReference: Boolean = rhsKeyOffset.isReference

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {

    if (input.isEmpty)
      return ClosingIterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return ClosingIterator.empty

    val table = buildProbeTable(input, state)
    state.query.resources.trace(table)

    // This will only happen if all the lhs-values evaluate to null, which is probably rare.
    // But, it's cheap to check and will save us from exhausting the rhs, so it's probably worth it
    if (table.isEmpty) {
      table.close()
      return ClosingIterator.empty
    }

    probeInput(rhsIterator, table, state.query)
  }

  private def buildProbeTable(
    lhsInput: ClosingIterator[CypherRow],
    queryState: QueryState
  ): LongProbeTable[CypherRow] = {
    val table = LongProbeTable.createLongProbeTable[CypherRow](
      queryState.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    )

    for (current <- lhsInput) {
      val nodeId = SlottedRow.getNodeId(current, lhsOffset, lhsIsReference)
      if (nodeId != -1) {
        current.compact()
        table.put(nodeId, current)
      }
    }

    table
  }

  private def probeInput(
    rhsInput: ClosingIterator[CypherRow],
    probeTable: LongProbeTable[CypherRow],
    queryContext: QueryContext
  ): ClosingIterator[CypherRow] =
    new PrefetchingIterator[CypherRow] {
      private var matches: util.Iterator[CypherRow] = util.Collections.emptyIterator()
      private var currentRhsRow: CypherRow = _

      override def produceNext(): Option[CypherRow] = {
        // If we have already found matches, we'll first exhaust these
        if (matches.hasNext) {
          val lhs = matches.next()
          val newRow = SlottedRow(slots)
          newRow.copyAllFrom(lhs)
          copyDataFromRow(rhsMappers, newRow, currentRhsRow, queryContext)
          return Some(newRow)
        }

        while (rhsInput.hasNext) {
          currentRhsRow = rhsInput.next()
          val nodeId = SlottedRow.getNodeId(currentRhsRow, rhsOffset, rhsIsReference)
          if (nodeId != -1) {
            val innerMatches = probeTable.get(nodeId)
            if (innerMatches.hasNext) {
              matches = innerMatches
              return produceNext()
            }
          }
        }

        // We have produced the last row, close the probe table to release estimated heap usage
        probeTable.close()

        None
      }

      override protected[this] def closeMore(): Unit = probeTable.close()
    }
}
