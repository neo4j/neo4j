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

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

case class PartialSortPipe(source: Pipe,
                           alreadySortedPrefix: Seq[ColumnOrder],
                           stillToSortSuffix: Seq[ColumnOrder])
                          (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {
  assert(alreadySortedPrefix.nonEmpty)
  assert(stillToSortSuffix.nonEmpty)

  private val prefixComparator = ExecutionContextOrdering.asComparator(alreadySortedPrefix)
  private val suffixComparator = ExecutionContextOrdering.asComparator(stillToSortSuffix)

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    new Iterator[ExecutionContext] {
      var state: PartialOrderState = PartialOrderState(new java.util.ArrayList[ExecutionContext](), 0, null)

      override def hasNext: Boolean = {
        (state.buffer.isEmpty && input.hasNext) ||
          state.nextIndexToRead < state.buffer.size() ||
          state.firstRowOfNextChunk != null
      }

      override def next(): ExecutionContext = {
        if (state.firstRowOfNextChunk == null && state.buffer.isEmpty) {
          // This is the very first call to next
          fillBufferAndReturnFirstRow(None)
        } else if (state.nextIndexToRead < state.buffer.size) {
          // We have a buffered row we can read
          val indexToRead = state.nextIndexToRead
          state.nextIndexToRead += 1
          state.buffer.get(indexToRead)
        } else {
          // We have to read the next chunk into the buffer
          fillBufferAndReturnFirstRow(Some(state.firstRowOfNextChunk))
        }
      }

      private def fillBufferAndReturnFirstRow(maybeFirstRow: Option[ExecutionContext]): ExecutionContext = {
        val buffer = state.buffer
        if (!buffer.isEmpty) {
          buffer.clear()
        }
        val first = maybeFirstRow.getOrElse(input.next())
        var current = first

        // Fill up buffer with all elements that are equal on the already sorted columns
        while (current != null && prefixComparator.compare(first, current) == 0) {
          buffer.add(current)
          if (input.hasNext) {
            current = input.next()
          } else {
            current = null
          }
        }

        // Sort this chunk
        buffer.sort(suffixComparator)

        // Update state
        state.nextIndexToRead = 1
        state.firstRowOfNextChunk = current

        // Return first row
        buffer.get(0)
      }
    }

  /**
    * This state holds all information needed between two calls to `next()`.
    * The buffer is reused and holds all rows of one chunk, i.e. where
    * the values of the already sorted columns are equal. We save the index
    * from which we have to read from the buffer on the next `next()` call.
    * And finally we will have read already the first row of the next chunk,
    * if there is a next chunk, and need to save it for later.
    *
    * @param buffer              the buffer
    * @param nextIndexToRead     the next index to read from the buffer
    * @param firstRowOfNextChunk the first row from the next chunk
    */
  case class PartialOrderState(buffer: java.util.ArrayList[ExecutionContext],
                               var nextIndexToRead: Int,
                               var firstRowOfNextChunk: ExecutionContext)
}
