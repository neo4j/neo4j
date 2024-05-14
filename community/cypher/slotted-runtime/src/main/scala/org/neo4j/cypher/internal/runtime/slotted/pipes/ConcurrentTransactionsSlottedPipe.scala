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

import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.runtime
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.JavaIteratorAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AbstractConcurrentTransactionsPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionForeachPipe.toStatusMap
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionStatus
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.EagerBuffer
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.storable.Values

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

abstract class AbstractConcurrentTransactionsSlottedPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  concurrency: Expression,
  onErrorBehaviour: InTransactionsOnErrorBehaviour,
  statusSlot: Option[Slot]
) extends AbstractConcurrentTransactionsPipe(source, inner, batchSize, concurrency, onErrorBehaviour) {

  private[this] val statusMapper = statusSlot.map(_.offset) match {
    case Some(statusOffset) => (output: runtime.ClosingIterator[CypherRow], status: TransactionStatus) => {
        output.withVariable(statusOffset, toStatusMap(status))
      }
    case _ => (output: runtime.ClosingIterator[CypherRow], _: TransactionStatus) => {
        output
      }
  }

  override protected def withStatus(
    output: ClosingIterator[CypherRow],
    status: TransactionStatus
  ): ClosingIterator[CypherRow] = statusMapper(output, status)
}

case class ConcurrentTransactionApplySlottedPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  concurrency: Expression,
  onErrorBehaviour: InTransactionsOnErrorBehaviour,
  nullableSlots: Set[Slot],
  statusSlot: Option[Slot]
)(val id: Id = Id.INVALID_ID)
    extends AbstractConcurrentTransactionsSlottedPipe(
      source,
      inner,
      batchSize,
      concurrency,
      onErrorBehaviour,
      statusSlot
    ) {
  private[this] val nullableLongOffsets = nullableSlots.toArray.collect { case LongSlot(offset, _, _) => offset }
  private[this] val nullableRefOffsets = nullableSlots.toArray.collect { case RefSlot(offset, _, _) => offset }

  override protected def nullRows(lhs: EagerBuffer[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    lhs.autoClosingIterator().asClosingIterator.map { row =>
      val nullRow = state.newRowWithArgument(rowFactory)
      nullRow.mergeWith(row, state.query)
      nullableLongOffsets.foreach(offset => nullRow.setLongAt(offset, -1L))
      nullableRefOffsets.foreach(offset => nullRow.setRefAt(offset, Values.NO_VALUE))
      nullRow
    }
  }

  override protected def createTask(
    innerPipe: TransactionPipeWrapper,
    batch: EagerBuffer[CypherRow],
    memoryTracker: MemoryTracker,
    state: QueryState,
    outputQueue: ArrayBlockingQueue[TaskOutputResult],
    activeTaskCount: AtomicInteger
  ): Runnable = {
    new ConcurrentTransactionApplyResultsTask(innerPipe, batch, memoryTracker, state, outputQueue, activeTaskCount)
  }
}

case class ConcurrentTransactionForeachSlottedPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  concurrency: Expression,
  onErrorBehaviour: InTransactionsOnErrorBehaviour,
  statusSlot: Option[Slot]
)(val id: Id = Id.INVALID_ID)
    extends AbstractConcurrentTransactionsSlottedPipe(
      source,
      inner,
      batchSize,
      concurrency,
      onErrorBehaviour,
      statusSlot
    ) {

  override protected def nullRows(lhs: EagerBuffer[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    throw new UnsupportedOperationException("ConcurrentTransactionForeachSlottedPipe does not support null rows")
  }

  override protected def createTask(
    innerPipe: TransactionPipeWrapper,
    batch: EagerBuffer[CypherRow],
    memoryTracker: MemoryTracker,
    state: QueryState,
    outputQueue: ArrayBlockingQueue[TaskOutputResult],
    activeTaskCount: AtomicInteger
  ): Runnable = {
    new ConcurrentTransactionForeachResultsTask(innerPipe, batch, memoryTracker, state, outputQueue, activeTaskCount)
  }
}
