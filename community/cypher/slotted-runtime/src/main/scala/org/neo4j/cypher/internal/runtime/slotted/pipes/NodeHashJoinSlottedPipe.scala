/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotAllocationFailed
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.ReadQueryContext
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.SlotMappings
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.KeyOffsets
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.SlotMapping
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.copyDataFromRow
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.fillKeyArray
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.ProbeTable
import org.neo4j.values.storable.LongArray
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.RelationshipValue
import org.neo4j.values.virtual.VirtualNodeValue

import java.util

case class NodeHashJoinSlottedPipe(
  lhsKeyOffsets: KeyOffsets,
  rhsKeyOffsets: KeyOffsets,
  left: Pipe,
  right: Pipe,
  slots: SlotConfiguration,
  rhsSlotMappings: SlotMappings
)(val id: Id = Id.INVALID_ID) extends AbstractHashJoinPipe[LongArray](left, right) {

  private val lhsOffsets: Array[Int] = lhsKeyOffsets.offsets
  private val lhsIsReference: Array[Boolean] = lhsKeyOffsets.isReference
  private val rhsOffsets: Array[Int] = rhsKeyOffsets.offsets
  private val rhsIsReference: Array[Boolean] = rhsKeyOffsets.isReference

  private val width: Int = lhsOffsets.length

  private val rhsMappings: Array[SlotMapping] = rhsSlotMappings.slotMapping
  private val rhsCachedPropertyMappings: Array[(Int, Int)] = rhsSlotMappings.cachedPropertyMappings

  override def buildProbeTable(
    lhsInput: ClosingIterator[CypherRow],
    queryState: QueryState
  ): ProbeTable[LongArray, CypherRow] = {
    val table = ProbeTable.createProbeTable[LongArray, CypherRow](
      queryState.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    )

    for (current <- lhsInput) {
      val key = new Array[Long](width)
      fillKeyArray(current, key, lhsOffsets, lhsIsReference)

      if (key(0) != -1) {
        table.put(Values.longArray(key), current)
      }
    }

    table
  }

  override def probeInput(
    rhsInput: ClosingIterator[CypherRow],
    queryState: QueryState,
    probeTable: ProbeTable[LongArray, CypherRow]
  ): ClosingIterator[CypherRow] =
    new PrefetchingIterator[CypherRow] {
      private val key = new Array[Long](width)
      private var matches: util.Iterator[CypherRow] = util.Collections.emptyIterator()
      private var currentRhsRow: CypherRow = _

      override def produceNext(): Option[CypherRow] = {
        // If we have already found matches, we'll first exhaust these
        if (matches.hasNext) {
          val lhs = matches.next()
          val newRow = SlottedRow(slots)
          newRow.copyAllFrom(lhs)
          copyDataFromRow(rhsMappings, rhsCachedPropertyMappings, newRow, currentRhsRow, queryState.query)
          return Some(newRow)
        }

        while (rhsInput.hasNext) {
          currentRhsRow = rhsInput.next()
          fillKeyArray(currentRhsRow, key, rhsOffsets, rhsIsReference)
          if (key(0) != -1 /*If we have nulls in the key, no match will be found*/ ) {
            matches = probeTable.get(Values.longArray(key))
            if (matches.hasNext) {
              // If we did not recurse back in like this, we would have to double up on the logic for creating output rows from matches
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

object NodeHashJoinSlottedPipe {

  case class SlotMapping(fromOffset: Int, toOffset: Int, fromIsLongSlot: Boolean, toIsLongSlot: Boolean)

  /**
   * Copies longs, refs, and cached properties from source row into target row.
   */
  def copyDataFromRow(
    slotMappings: Array[SlotMapping],
    cachedPropertyMappings: Array[(Int, Int)],
    target: WritableRow,
    source: ReadableRow,
    queryContext: ReadQueryContext
  ): Unit = {

    var i = 0
    while (i < slotMappings.length) {
      slotMappings(i) match {
        case SlotMapping(fromOffset, toOffset, true, true)   => target.setLongAt(toOffset, source.getLongAt(fromOffset))
        case SlotMapping(fromOffset, toOffset, false, false) => target.setRefAt(toOffset, source.getRefAt(fromOffset))
        case SlotMapping(fromOffset, toOffset, false, true) =>
          target.setLongAt(
            toOffset,
            source.getRefAt(fromOffset) match {
              case v: VirtualNodeValue  => v.id()
              case v: RelationshipValue => v.id()
            }
          )
        case SlotMapping(fromOffset, toOffset, true, false) =>
          target.setRefAt(toOffset, queryContext.nodeById(source.getLongAt(fromOffset)))
      }
      i += 1
    }
    i = 0
    while (i < cachedPropertyMappings.length) {
      val (from, to) = cachedPropertyMappings(i)
      target.setCachedPropertyAt(to, source.getCachedPropertyAt(from))
      i += 1
    }
  }

  /**
   * Modifies the given key array by writing the ids of the nodes
   * at the offsets of the given execution context into the array.
   *
   * If at least one node is null. It will write -1 into the first
   * position of the array.
   */
  def fillKeyArray(current: ReadableRow, key: Array[Long], offsets: Array[Int], isReference: Array[Boolean]): Unit = {
    // We use a while loop like this to be able to break out early
    var i = 0
    while (i < offsets.length) {
      val thisId = SlottedRow.getNodeId(current, offsets(i), isReference(i))
      key(i) = thisId
      if (NullChecker.entityIsNull(thisId)) {
        key(0) = NullChecker.NULL_ENTITY // We flag the null in this cryptic way to avoid creating objects
        return
      }
      i += 1
    }
  }

  case class KeyOffsets private (
    offsets: Array[Int],
    isReference: Array[Boolean]
  ) {
    def isSingle: Boolean = offsets.length == 1
    def asSingle: SingleKeyOffset = SingleKeyOffset(offsets(0), isReference(0))
  }

  object KeyOffsets {

    def create(slots: SlotConfiguration, keyVariables: Array[String]): KeyOffsets = {
      val (offsets, isReference) =
        keyVariables.map(k =>
          slots.get(k)
            .getOrElse(throw new SlotAllocationFailed(s"No slot for variable $k"))
        )
          .map(slot => (slot.offset, !slot.isLongSlot))
          .unzip

      KeyOffsets(offsets, isReference)
    }

    def longs(offsets: Int*): KeyOffsets = KeyOffsets(offsets.toArray, Array.fill(offsets.size)(false))
  }

  case class SingleKeyOffset(
    offset: Int,
    isReference: Boolean
  )
}
