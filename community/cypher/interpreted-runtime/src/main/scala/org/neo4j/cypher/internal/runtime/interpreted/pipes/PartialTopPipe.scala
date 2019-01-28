/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
                          (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  countExpression.registerOwningPipe(this)

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

        new Iterator[ExecutionContext] {
          var state: PartialTopState = PartialTopBufferState(new java.util.ArrayList[ExecutionContext](), 0, null, longCount)

          override def hasNext: Boolean = {
            state match {
              case PartialTopBufferState(buffer, nextIndexToRead, firstRowOfNextChunk, remainingLimit) =>
                (buffer.isEmpty && restoredInput.hasNext) ||
                  nextIndexToRead < buffer.size() ||
                  (firstRowOfNextChunk != null && remainingLimit > 0)
              case PartialTopTableState(tableIterator) =>
                tableIterator.hasNext
            }
          }

          override def next(): ExecutionContext = {
            state match {
              case state@PartialTopBufferState(buffer, nextIndexToRead, firstRowOfNextChunk, _) =>
                if (firstRowOfNextChunk == null && buffer.isEmpty) {
                  // This is the very first call to next
                  fillBufferAndReturnFirstRow(state)
                } else if (nextIndexToRead < buffer.size) {
                  // We have a buffered row we can read
                  val indexToRead = nextIndexToRead
                  state.nextIndexToRead += 1
                  buffer.get(indexToRead)
                } else {
                  // We have to read the next chunk into the buffer
                  fillBufferAndReturnFirstRow(state)
                }
              case PartialTopTableState(tableIterator) =>
                tableIterator.next()
            }

          }

          private def fillBufferAndReturnFirstRow(bufferState: PartialTopBufferState): ExecutionContext = {
            val buffer = bufferState.buffer
            var topTable: DefaultComparatorTopTable[ExecutionContext] = null
            if (!buffer.isEmpty) {
              buffer.clear()
            }
            val first = if (bufferState.firstRowOfNextChunk != null) bufferState.firstRowOfNextChunk else restoredInput.next()
            var current = first

            // Fill up buffer/TopTable with all elements that are equal on the already sorted columns
            while (current != null && prefixComparator.compare(first, current) == 0) {

              // add to either Buffer or TopTable
              if (bufferState.remainingLimit > 0) {
                bufferState.remainingLimit -= 1
                buffer.add(current)
              } else {
                if (topTable == null) {
                  // At this point we switch from a buffer for the whole chunk to a TopTable
                  topTable = new DefaultComparatorTopTable[ExecutionContext](suffixComparator, buffer.size())
                  // Transfer everything buffered so far into the TopTable
                  var i = 0
                  while (i < buffer.size()) {
                    topTable.add(buffer.get(i))
                    i += 1
                  }
                  // Clean up the buffer
                  buffer.clear()
                }
                // Add the current row to the TopTable
                topTable.add(current)
              }

              if (restoredInput.hasNext) {
                current = restoredInput.next()
              } else {
                current = null
              }
            }

            if (topTable == null) {
              if (buffer.size() > 1) {
                // Sort the buffered chunk
                buffer.sort(suffixComparator)
              }

              // Update state
              bufferState.nextIndexToRead = 1
              bufferState.firstRowOfNextChunk = current

              // Return first row
              buffer.get(0)
            } else {
              topTable.sort()
              val iterator = topTable.iterator().asScala
              // Update the state
              state = PartialTopTableState(iterator)
              iterator.next()
            }

          }
        }
      }
    }
  }

  trait PartialTopState

  /**
    * We are currently reading incoming rows into a buffer.
    * The buffer is reused and holds all rows of one chunk, i.e. where
    * the values of the already sorted columns are equal. We save the index
    * from which we have to read from the buffer on the next `next()` call.
    * And finally we will have read already the first row of the next chunk,
    * if there is a next chunk, and need to save it for later.
    *
    * @param buffer              the buffer
    * @param nextIndexToRead     the next index to read from the buffer
    * @param firstRowOfNextChunk the first row from the next chunk
    * @param remainingLimit      how many rows are left after this chunk until reaching the limit
    */
  case class PartialTopBufferState(buffer: java.util.ArrayList[ExecutionContext],
                                   var nextIndexToRead: Int,
                                   var firstRowOfNextChunk: ExecutionContext,
                                   var remainingLimit: Long) extends PartialTopState

  /**
    * We have reached the case that the ramining limit is smaller than the current chunk size.
    * Therefore, we read everything into a TopTable of the right size and stream from the
    * resulting iterator.
    *
    * @param tableIterator the iterator of the TopTable
    */
  case class PartialTopTableState(tableIterator: Iterator[ExecutionContext]) extends PartialTopState
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
