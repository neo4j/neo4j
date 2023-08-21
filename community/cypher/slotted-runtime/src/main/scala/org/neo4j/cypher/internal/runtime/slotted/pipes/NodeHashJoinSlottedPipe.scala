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

import org.neo4j.cypher.internal.expressions.LogicalVariable
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
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.SlotMapper
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.SlotMappers
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.copyDataFromRow
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.fillKeyArray
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.ProbeTable
import org.neo4j.values.storable.LongArray
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

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

  private val rhsMappers: Array[SlotMapper] = SlotMappers(rhsSlotMappings)

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
        current.compact()
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
          copyDataFromRow(rhsMappers, newRow, currentRhsRow, queryState.query)
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

  object SlotMappers {

    def apply(mappings: SlotMappings): Array[SlotMapper] = {
      val slots = mappings.slotMapping.iterator
        .map(SlotMapper.apply)
        .map(toMultiMapperIfPossible)
        .foldLeft(List.empty[SlotMapper])(combineMultiMappers) // Combine adjacent slot mappings
        .map(toSingleMapperIfPossible)
        .reverse

      val cachedProps = mappings.cachedPropertyMappings.iterator
        .map { case (from, to) => CachedPropSlotMapper(from, to) }

      (slots ++ cachedProps).toArray
    }

    private def combineMultiMappers(previous: List[SlotMapper], current: SlotMapper): List[SlotMapper] = {
      (previous, current) match {
        case ((last: MultiLongSlotMapper) :: tail, next: MultiLongSlotMapper) if canCombine(last, next) =>
          last.copy(length = last.length + next.length) :: tail
        case ((last: MultiRefSlotMapper) :: tail, next: MultiRefSlotMapper) if canCombine(last, next) =>
          last.copy(length = last.length + next.length) :: tail
        case (acc, mapper) =>
          mapper :: acc
      }
    }

    private def toMultiMapperIfPossible(mapper: SlotMapper): SlotMapper = {
      mapper match {
        case LongSlotMapper(fromOffset, toOffset) => MultiLongSlotMapper(fromOffset, toOffset, 1)
        case RefSlotMapper(fromOffset, toOffset)  => MultiRefSlotMapper(fromOffset, toOffset, 1)
        case other                                => other
      }
    }

    private def toSingleMapperIfPossible(mapper: SlotMapper): SlotMapper = {
      mapper match {
        case MultiLongSlotMapper(fromOffset, toOffset, 1) => LongSlotMapper(fromOffset, toOffset)
        case MultiRefSlotMapper(fromOffset, toOffset, 1)  => RefSlotMapper(fromOffset, toOffset)
        case other                                        => other
      }
    }

    private def canCombine(a: MultiLongSlotMapper, b: MultiLongSlotMapper): Boolean = {
      a.fromOffset + a.length == b.fromOffset && a.toOffset + a.length == b.toOffset
    }

    private def canCombine(a: MultiRefSlotMapper, b: MultiRefSlotMapper): Boolean = {
      a.fromOffset + a.length == b.fromOffset && a.toOffset + a.length == b.toOffset
    }
  }

  object SlotMapper {

    def apply(mapping: SlotMapping): SlotMapper = {
      mapping match {
        case SlotMapping(fromOffset, toOffset, true, true)   => LongSlotMapper(fromOffset, toOffset)
        case SlotMapping(fromOffset, toOffset, false, false) => RefSlotMapper(fromOffset, toOffset)
        case SlotMapping(fromOffset, toOffset, false, true)  => RefToLongSlotMapper(fromOffset, toOffset)
        case SlotMapping(fromOffset, toOffset, true, false)  => LongToRefSlotMapper(fromOffset, toOffset)
      }
    }
  }

  trait SlotMapper {
    def copyRow(read: ReadQueryContext, source: ReadableRow, target: WritableRow): Unit
  }

  case class LongSlotMapper(fromOffset: Int, toOffset: Int) extends SlotMapper {

    override def copyRow(read: ReadQueryContext, source: ReadableRow, target: WritableRow): Unit = {
      target.setLongAt(toOffset, source.getLongAt(fromOffset))
    }
  }

  case class MultiLongSlotMapper(fromOffset: Int, toOffset: Int, length: Int) extends SlotMapper {

    override def copyRow(read: ReadQueryContext, source: ReadableRow, target: WritableRow): Unit = {
      target.copyLongsFrom(source, fromOffset, toOffset, length)
    }
  }

  case class RefSlotMapper(fromOffset: Int, toOffset: Int) extends SlotMapper {

    override def copyRow(read: ReadQueryContext, source: ReadableRow, target: WritableRow): Unit = {
      target.setRefAt(toOffset, source.getRefAt(fromOffset))
    }
  }

  case class MultiRefSlotMapper(fromOffset: Int, toOffset: Int, length: Int) extends SlotMapper {

    override def copyRow(read: ReadQueryContext, source: ReadableRow, target: WritableRow): Unit = {
      target.copyRefsFrom(source, fromOffset, toOffset, length)
    }
  }

  case class LongToRefSlotMapper(fromOffset: Int, toOffset: Int) extends SlotMapper {

    override def copyRow(read: ReadQueryContext, source: ReadableRow, target: WritableRow): Unit = {
      // TODO How do we know it's a node and not a relationship?
      target.setRefAt(toOffset, read.nodeById(source.getLongAt(fromOffset)))
    }
  }

  case class RefToLongSlotMapper(fromOffset: Int, toOffset: Int) extends SlotMapper {

    override def copyRow(read: ReadQueryContext, source: ReadableRow, target: WritableRow): Unit = {
      source.getRefAt(fromOffset) match {
        case node: VirtualNodeValue        => target.setLongAt(toOffset, node.id())
        case rel: VirtualRelationshipValue => target.setLongAt(toOffset, rel.id())
      }
    }
  }

  case class CachedPropSlotMapper(fromOffset: Int, toOffset: Int) extends SlotMapper {

    override def copyRow(read: ReadQueryContext, source: ReadableRow, target: WritableRow): Unit = {
      target.setCachedPropertyAt(toOffset, source.getCachedPropertyAt(fromOffset))
    }
  }

  /**
   * Copies longs, refs, and cached properties from source row into target row.
   */
  def copyDataFromRow(
    slotMappings: Array[SlotMapper],
    target: WritableRow,
    source: ReadableRow,
    queryContext: ReadQueryContext
  ): Unit = {

    var i = 0
    while (i < slotMappings.length) {
      slotMappings(i).copyRow(queryContext, source, target)
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

    def create(slots: SlotConfiguration, keyVariables: Array[LogicalVariable]): KeyOffsets = {
      val (offsets, isReference) =
        keyVariables.map(slots.apply)
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
