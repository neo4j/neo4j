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

import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.JavaIteratorAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionForeachPipe.toStatusMap
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.EagerBuffer
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

abstract class AbstractConcurrentTransactionsLegacyPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  concurrency: Option[Expression],
  onErrorBehaviour: InTransactionsOnErrorBehaviour,
  statusVariableOpt: Option[String]
) extends AbstractConcurrentTransactionsPipe(source, inner, batchSize, concurrency, onErrorBehaviour) {

  override protected def withStatus(
    output: ClosingIterator[CypherRow],
    status: TransactionStatus
  ): ClosingIterator[CypherRow] = statusVariableOpt match {
    case Some(statusVariable) => output.withVariable(statusVariable, toStatusMap(status))
    case _                    => output
  }
}

case class ConcurrentTransactionApplyLegacyPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  concurrency: Option[Expression],
  onErrorBehaviour: InTransactionsOnErrorBehaviour,
  nullableVariables: Set[String],
  statusVariableOpt: Option[String]
)(val id: Id = Id.INVALID_ID)
    extends AbstractConcurrentTransactionsLegacyPipe(
      source,
      inner,
      batchSize,
      concurrency,
      onErrorBehaviour,
      statusVariableOpt
    ) {

  private lazy val nullEntries: Seq[(String, AnyValue)] = {
    nullableVariables.toIndexedSeq.map(name => name -> Values.NO_VALUE)
  }

  override protected def nullRows(lhs: EagerBuffer[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    lhs.autoClosingIterator().asClosingIterator.map { row =>
      val nullRow = state.newRowWithArgument(rowFactory)
      nullRow.mergeWith(row, state.query)
      nullRow.set(nullEntries)
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

case class ConcurrentTransactionForeachLegacyPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  concurrency: Option[Expression],
  onErrorBehaviour: InTransactionsOnErrorBehaviour,
  statusVariableOpt: Option[String]
)(val id: Id = Id.INVALID_ID)
    extends AbstractConcurrentTransactionsLegacyPipe(
      source,
      inner,
      batchSize,
      concurrency,
      onErrorBehaviour,
      statusVariableOpt
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
