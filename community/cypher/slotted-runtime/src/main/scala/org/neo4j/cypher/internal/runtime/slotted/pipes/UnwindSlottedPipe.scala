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
import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue

import scala.annotation.tailrec

case class UnwindSlottedPipe(source: Pipe, collection: Expression, offset: Int, slots: SlotConfiguration)(val id: Id =
  Id.INVALID_ID) extends PipeWithSource(source) with ListSupport {

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] =
    new UnwindIterator(input, state)

  private class UnwindIterator(input: ClosingIterator[CypherRow], state: QueryState)
      extends ClosingIterator[CypherRow] {
    private[this] var currentInputRow: CypherRow = _
    private[this] var unwindIterator: java.util.Iterator[AnyValue] = _
    private[this] var nextItem: SlottedRow = _
    private[this] var rowsEmitted: Long = 0L

    prefetch()

    override def innerHasNext: Boolean = nextItem != null

    override def next(): CypherRow =
      if (hasNext) {
        val ret = nextItem
        prefetch()
        // check in from time to time to make sure no one has closed the transaction
        if (rowsEmitted & UnwindIterator.CHECK_TX_INTERVAL == 0) {
          state.query.transactionalContext.assertTransactionOpen()
        }
        rowsEmitted += 1L
        ret
      } else {
        Iterator.empty.next() // Fail nicely
      }

    @tailrec
    private def prefetch(): Unit = {
      nextItem = null
      if (unwindIterator != null && unwindIterator.hasNext) {
        val newItem = SlottedRow(slots)
        newItem.copyAllFrom(currentInputRow)
        newItem.setRefAt(offset, unwindIterator.next())
        nextItem = newItem
      } else {
        if (input.hasNext) {
          val newCurrent = input.next()
          val value: AnyValue = collection(newCurrent, state)
          currentInputRow = newCurrent
          unwindIterator = makeTraversable(value).iterator
          prefetch()
        }
      }
    }

    override protected[this] def closeMore(): Unit = input.close()
  }

  private object UnwindIterator {
    // Specifies how often we should verify that the transaction is still open
    // NOTE, if you modify this make sure to pick a value that satisfies 2^n - 1
    private val CHECK_TX_INTERVAL: Int = 127
  }
}
