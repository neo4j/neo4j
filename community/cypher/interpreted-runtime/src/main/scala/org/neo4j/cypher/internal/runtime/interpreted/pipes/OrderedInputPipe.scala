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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow

/**
 * A pipe that relies on its input coming in a certain order and processing that input in chunks.
 */
trait OrderedInputPipe {
  self: PipeWithSource =>

  /**
   * @return provide the receiver that will be used to create results. If you override internalCreateResults, and choose to call
   *         [[internalCreateResultsWithReceiver]] directly, you do not have to implement this method.
   */
  def getReceiver(state: QueryState): OrderedChunkReceiver

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val receiver = getReceiver(state)
    internalCreateResultsWithReceiver(input, receiver)
  }

  /**
   * Processes the input in chunks as described by [[OrderedChunkReceiver]].
   */
  final protected def internalCreateResultsWithReceiver(
    input: ClosingIterator[CypherRow],
    receiver: OrderedChunkReceiver
  ): ClosingIterator[CypherRow] = {
    val inputState = new InputState()

    new ClosingIterator[CypherRow] {
      private var processNextChunk = true

      override protected[this] def closeMore(): Unit = {
        receiver.close()
        input.close()
      }

      override def innerHasNext: Boolean = {
        val _hasNext = inputState.resultRowsOfChunk.hasNext ||
          (processNextChunk && (
            inputState.firstRowOfNextChunk != null || input.hasNext
          ))
        if (!_hasNext) {
          receiver.close()
        }
        _hasNext
      }

      override def next(): CypherRow = {
        if (inputState.firstRowOfNextChunk == null && inputState.resultRowsOfChunk.isEmpty) {
          populateTableAndReturnFirstRow(null)
        } else if (inputState.resultRowsOfChunk.hasNext) {
          inputState.resultRowsOfChunk.next()
        } else {
          populateTableAndReturnFirstRow(inputState.firstRowOfNextChunk)
        }
      }

      def populateTableAndReturnFirstRow(maybeFirstRow: CypherRow): CypherRow = {
        receiver.clear()
        val firstRow = if (maybeFirstRow == null) input.next() else maybeFirstRow
        var currentRow = firstRow

        while (currentRow != null && receiver.isSameChunk(firstRow, currentRow)) {
          receiver.processRow(currentRow)
          if (input.hasNext) {
            currentRow = input.next()
          } else {
            currentRow = null
          }
        }

        inputState.firstRowOfNextChunk = currentRow
        inputState.resultRowsOfChunk = receiver.result()
        processNextChunk = receiver.processNextChunk
        inputState.resultRowsOfChunk.next()
      }
    }
  }

  private class InputState() {
    var firstRowOfNextChunk: CypherRow = _
    var resultRowsOfChunk: ClosingIterator[CypherRow] = ClosingIterator.empty
  }

}

/**
 * A receiver needs to process rows in chunks. The call pattern looks like:
 *
 * clear()
 * isSameChunk(a, a) -> true
 * processRow(a)
 * isSameChunk(a, b) -> true
 * processRow(b)
 * isSameChunk(a, c) -> false
 * result()
 * processNextChunk -> true
 *
 * clear()
 * ...
 * processNextChunk -> false
 */
trait OrderedChunkReceiver {

  /**
   * Will be called after a chunk is completed and all rows streamed out to the enclosing Pipe.
   */
  def clear(): Unit

  /**
   * @param first   the first row of the current chunk
   * @param current the row in question.
   * @return if the two rows are part of the same chunk.
   */
  def isSameChunk(first: CypherRow, current: CypherRow): Boolean

  /**
   * Called for each row.
   */
  def processRow(row: CypherRow): Unit

  /**
   * Called at the end of a chunk.
   *
   * @return an iterator of rows.
   */
  def result(): ClosingIterator[CypherRow]

  /**
   * @return whether the next chunk needs to be processed
   */
  def processNextChunk: Boolean

  /**
   * Will be called after all chunks are completed.
   */
  def close(): Unit
}
