/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util

import org.eclipse.collections.api.multimap.list.MutableListMultimap
import org.eclipse.collections.impl.factory.Multimaps
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.opencypher.v9_0.util.attribution.Id

case class NodeHashJoinSlottedPipe(lhsOffsets: Array[Int],
                                   rhsOffsets: Array[Int],
                                   left: Pipe,
                                   right: Pipe,
                                   slots: SlotConfiguration,
                                   longsToCopy: Array[(Int, Int)],
                                   refsToCopy: Array[(Int, Int)])
                                  (val id: Id = Id.INVALID_ID) extends PipeWithSource(left) {
  private val width: Int = lhsOffsets.length

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {

    if (input.isEmpty)
      return Iterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return Iterator.empty

    val table = buildProbeTable(input, state)

    // This will only happen if all the lhs-values evaluate to null, which is probably rare.
    // But, it's cheap to check and will save us from exhausting the rhs, so it's probably worth it
    if (table.isEmpty)
      return Iterator.empty

    probeInput(rhsIterator, state, table)
  }

  private def buildProbeTable(lhsInput: Iterator[ExecutionContext], queryState: QueryState): MutableListMultimap[Key, ExecutionContext] = {
    val table = Multimaps.mutable.list.empty[Key, ExecutionContext]()

    for (current <- lhsInput) {
      val key = new Array[Long](width)
      fillKeyArray(current, key, lhsOffsets)

      if (key(0) != -1)
        table.put(new Key(key), current)
    }

    table
  }

  private def probeInput(rhsInput: Iterator[ExecutionContext],
                         queryState: QueryState,
                         probeTable: MutableListMultimap[Key, ExecutionContext]): Iterator[ExecutionContext] =
    new PrefetchingIterator[ExecutionContext] {
      private val key = new Array[Long](width)
      private var matches: util.Iterator[ExecutionContext] = util.Collections.emptyIterator()
      private var currentRhsRow: ExecutionContext = _

      override def produceNext(): Option[ExecutionContext] = {
        // If we have already found matches, we'll first exhaust these
        if (matches.hasNext) {
          val lhs = matches.next()
          val newRow = SlottedExecutionContext(slots)
          lhs.copyTo(newRow)
          copyDataFromRhs(newRow, currentRhsRow)
          return Some(newRow)
        }

        while (rhsInput.nonEmpty) {
          currentRhsRow = rhsInput.next()
          fillKeyArray(currentRhsRow, key, rhsOffsets)
          if (key(0) != -1 /*If we have nulls in the key, no match will be found*/ ) {
            matches = probeTable.get(new Key(key)).iterator()
            if (matches.hasNext) {
              // If we did not recurse back in like this, we would have to double up on the logic for creating output rows from matches
              return produceNext()
            }
          }
        }

        None
      }
    }

  private def fillKeyArray(current: ExecutionContext, key: Array[Long], offsets: Array[Int]): Unit = {
    // We use a while loop like this to be able to break out early
    var i = 0
    var containsNull = false
    while (i < width) {
      val thisId = current.getLongAt(offsets(i))
      key(i) = thisId
      if (thisId == -1 /*This is how we encode null nodes*/ ) {
        i = width
        containsNull = true
      }
      i += 1
    }
    if (containsNull)
      key(0) = -1 // We flag the null in this cryptic way to avoid creating objects
  }

  private def copyDataFromRhs(newRow: SlottedExecutionContext, rhs: ExecutionContext): Unit = {
    longsToCopy foreach {
      case (from, to) => newRow.setLongAt(to, rhs.getLongAt(from))
    }
    refsToCopy foreach {
      case (from, to) => newRow.setRefAt(to, rhs.getRefAt(from))
    }
  }
}
