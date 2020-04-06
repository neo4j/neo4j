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

import java.util.Comparator

import org.neo4j.cypher.internal.DefaultComparatorTopTable
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.values.storable.NumberValue

import scala.collection.JavaConverters._

case class PartialTopNPipe(source: Pipe,
                           countExpression: Expression,
                           prefixComparator: Comparator[ExecutionContext],
                           suffixComparator: Comparator[ExecutionContext])
                          (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source: Pipe) with OrderedInputPipe {

  countExpression.registerOwningPipe(this)

  override def getReceiver(state: QueryState): OrderedChunkReceiver = throw new IllegalStateException()

  class TopNReceiver(var remainingLimit: Long, state: QueryState) extends OrderedChunkReceiver {
    private val buffer = new java.util.ArrayList[ExecutionContext]()
    private var topTable: DefaultComparatorTopTable[ExecutionContext] = _

    override def clear(): Unit = {
      buffer.forEach(state.memoryTracker.deallocated)
      buffer.clear()
    }

    override def isSameChunk(first: ExecutionContext, current: ExecutionContext): Boolean = prefixComparator.compare(first, current) == 0

    override def processRow(row: ExecutionContext): Unit = {
      // add to either Buffer or TopTable
      if (remainingLimit > 0) {
        remainingLimit -= 1
        buffer.add(row)
        state.memoryTracker.allocated(row)
      } else {
        if (topTable == null) {
          // At this point we switch from a buffer for the whole chunk to a TopTable
          topTable = new DefaultComparatorTopTable[ExecutionContext](suffixComparator, buffer.size())
          // Transfer everything buffered so far into the TopTable
          var i = 0
          while (i < buffer.size()) {
            topTable.add(buffer.get(i))
            // Only calling allocated here for the rows that are already buffered,
            // and not for any rows after that, makes the assumption that rows have more or less the same size.
            // We don't know which ones are actually kept in the TopTable.
            state.memoryTracker.allocated(row)
            i += 1
          }
          // Clean up the buffer
          clear()
        }
        // Add the current row to the TopTable
        topTable.add(row)
      }
    }

    override def result(): Iterator[ExecutionContext] = {
      if (topTable == null) {
        if (buffer.size() > 1) {
          // Sort the buffered chunk
          buffer.sort(suffixComparator)
        }
        buffer.iterator().asScala
      } else {
        topTable.sort()
        topTable.iterator().asScala
      }
    }

    override def processNextChunk: Boolean = remainingLimit > 0
  }

  protected override def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    if (input.isEmpty) {
      Iterator.empty
    } else {
      val first = input.next()
      val longCount = countExpression(first, state).asInstanceOf[NumberValue].longValue()
      if (longCount <= 0) {
        Iterator.empty
      } else {
        // We don't need a special case for LIMIT > Int.Max (Like the TopPipe)
        // We use something similar to PartialSort in any way, and that will only fail at runtime if one chunk is too big.

        // We have to re-attach the already read first row to the iterator
        val restoredInput = Iterator.single(first) ++ input
        val receiver = new TopNReceiver(longCount, state)
        internalCreateResultsWithReceiver(restoredInput, state, receiver)
      }
    }
  }
}

/*
 * Special case for when we only have one element, in this case it is no idea to store
 * an array, instead just store a single value.
 */
case class PartialTop1Pipe(source: Pipe, prefixComparator: Comparator[ExecutionContext], suffixComparator: Comparator[ExecutionContext])
                          (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  protected override def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    if (input.isEmpty) {
      Iterator.empty
    } else {
      val first = input.next()
      var current = first
      var result = first

      while (current != null) {
        if (input.hasNext) {
          current = input.next()
          if (prefixComparator.compare(first, current) != 0) {
            // We can stop looking
            current = null
          } else if (suffixComparator.compare(current, result) < 0) {
            result = current
          }
        } else {
          current = null
        }
      }
      Iterator.single(result)
    }
  }
}

/*
 * Special case for when we only want one element, and all others that have the same value (tied for first place)
 */
case class PartialTop1WithTiesPipe(source: Pipe, prefixComparator: Comparator[ExecutionContext], suffixComparator: Comparator[ExecutionContext])
                                  (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  protected override def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    if (input.isEmpty) {
      Iterator.empty
    } else {
      val first = input.next()
      var current = first
      var best = first
      var matchingRows = init(best)

      while (current != null) {
        if (input.hasNext) {
          current = input.next()

          if (prefixComparator.compare(first, current) != 0) {
            // We can stop looking
            current = null
          } else {
            val comparison = suffixComparator.compare(current, best)
            if (comparison < 0) { // Found a new best
              best = current
              matchingRows.clear()
              matchingRows += current
            }

            if (comparison == 0) { // Found a tie
              matchingRows += current
            }
          }
        } else {
          current = null
        }
      }
      matchingRows.result().iterator
    }
  }

  @inline
  private def init(first: ExecutionContext) = {
    val builder = Vector.newBuilder[ExecutionContext]
    builder += first
    builder
  }
}
