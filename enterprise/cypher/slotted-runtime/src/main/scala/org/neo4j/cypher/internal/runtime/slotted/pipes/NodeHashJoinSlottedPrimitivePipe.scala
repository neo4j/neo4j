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

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap
import org.eclipse.collections.api.multimap.list.MutableListMultimap
import org.eclipse.collections.impl.factory.Multimaps
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps
import org.eclipse.collections.impl.list.mutable.FastList
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.opencypher.v9_0.util.attribution.Id

case class NodeHashJoinSlottedPrimitivePipe(lhsOffset: Int,
                                            rhsOffset: Int,
                                            left: Pipe,
                                            right: Pipe,
                                            slots: SlotConfiguration,
                                            longsToCopy: Array[(Int, Int)],
                                            refsToCopy: Array[(Int, Int)])
                                           (val id: Id = Id.INVALID_ID) extends PipeWithSource(left) {
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

  private def buildProbeTable(lhsInput: Iterator[ExecutionContext], queryState: QueryState): MutableLongObjectMap[FastList[ExecutionContext]] = {
    val table = LongObjectMaps.mutable.empty[FastList[ExecutionContext]]()

    for (current <- lhsInput) {
      val nodeId = current.getLongAt(lhsOffset)
      if(nodeId != -1) {
        val list = table.getIfAbsentPut(nodeId, new FastList[ExecutionContext](1))
        list.add(current)
      }
    }

    table
  }

  private def probeInput(rhsInput: Iterator[ExecutionContext],
                         queryState: QueryState,
                         probeTable: MutableLongObjectMap[FastList[ExecutionContext]]): Iterator[ExecutionContext] =
    new PrefetchingIterator[ExecutionContext] {
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
          val nodeId = currentRhsRow.getLongAt(rhsOffset)
          if(nodeId != -1) {
            val innerMatches = probeTable.get(nodeId)
            if(innerMatches != null) {
              matches = innerMatches.iterator()
              return produceNext()
            }
          }
        }

        None
      }
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
