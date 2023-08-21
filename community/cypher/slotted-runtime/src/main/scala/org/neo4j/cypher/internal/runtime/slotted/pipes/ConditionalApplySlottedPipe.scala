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

import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.Values

abstract class BaseConditionalApplySlottedPipe(
  lhs: Pipe,
  rhs: Pipe,
  longOffsets: Seq[Int],
  refOffsets: Seq[Int],
  slots: SlotConfiguration,
  nullableSlots: Seq[Slot]
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(lhs) with Pipe {

  // ===========================================================================
  // Compile-time initializations
  // ===========================================================================
  private val setNullableSlotToNullFunctions =
    nullableSlots.map({
      case LongSlot(offset, _, _) =>
        (context: CypherRow) => context.setLongAt(offset, -1L)
      case RefSlot(offset, _, _) =>
        (context: CypherRow) => context.setRefAt(offset, Values.NO_VALUE)
    })

  // ===========================================================================
  // Runtime code
  // ===========================================================================
  private def setNullableSlotsToNull(context: CypherRow): Unit = {
    val functions = setNullableSlotToNullFunctions.iterator
    while (functions.hasNext) {
      functions.next()(context)
    }
  }

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] =
    input.flatMap {
      lhsContext =>
        if (condition(lhsContext)) {
          val rhsState = state.withInitialContext(lhsContext)
          rhs.createResults(rhsState)
        } else {
          val output = SlottedRow(slots)
          setNullableSlotsToNull(output)
          output.copyAllFrom(lhsContext)
          ClosingIterator.single(output)
        }
    }

  def condition(context: CypherRow): Boolean
}

case class ConditionalApplySlottedPipe(
  lhs: Pipe,
  rhs: Pipe,
  longOffsets: Array[Int],
  refOffsets: Array[Int],
  slots: SlotConfiguration,
  nullableSlots: Seq[Slot]
)(id: Id = Id.INVALID_ID)
    extends BaseConditionalApplySlottedPipe(lhs, rhs, longOffsets, refOffsets, slots, nullableSlots)(id) {

  override def condition(context: CypherRow): Boolean = {
    var i = 0
    while (i < longOffsets.length) {
      if (entityIsNull(context.getLongAt(longOffsets(i)))) {
        return false
      }
      i += 1
    }
    i = 0
    while (i < refOffsets.length) {
      if (context.getRefAt(refOffsets(i)) eq Values.NO_VALUE) {
        return false
      }
      i += 1
    }
    true
  }
}

case class AntiConditionalApplySlottedPipe(
  lhs: Pipe,
  rhs: Pipe,
  longOffsets: Array[Int],
  refOffsets: Array[Int],
  slots: SlotConfiguration,
  nullableSlots: Seq[Slot]
)(id: Id = Id.INVALID_ID)
    extends BaseConditionalApplySlottedPipe(lhs, rhs, longOffsets, refOffsets, slots, nullableSlots)(id) {

  override def condition(context: CypherRow): Boolean = {
    var i = 0
    while (i < longOffsets.length) {
      if (!entityIsNull(context.getLongAt(longOffsets(i)))) {
        return false
      }
      i += 1
    }
    i = 0
    while (i < refOffsets.length) {
      if (!(context.getRefAt(refOffsets(i)) eq Values.NO_VALUE)) {
        return false
      }
      i += 1
    }
    true
  }
}
