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

import org.neo4j.cypher.internal.logical.plans.TransactionalPlan.RecoveryMode
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.JavaIteratorAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionForeachPipe.toStatusMap
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.EagerBuffer
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NoValue.NO_VALUE
import org.neo4j.values.storable.Values.booleanValue
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder

abstract class AbstractTransactionForeachPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  recoveryMode: RecoveryMode,
  retryPolicy: TransactionRetryPolicy
) extends AbstractSerialTransactionsPipe(source, inner, batchSize, recoveryMode, retryPolicy) {

  override protected def produceOutput(
    eagerBuffer: EagerBuffer[CypherRow],
    result: TransactionResult,
    batch: TransactionBatch,
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val output = eagerBuffer.autoClosingIterator().asClosingIterator
    withStatus(output, result.status)
  }

  override protected def getResult(
    innerPipeInTx: TransactionPipeWrapper,
    state: QueryState,
    batch: TransactionBatch,
    memoryTracker: MemoryTracker
  ): TransactionResult =
    innerPipeInTx.consume(state, batch)
}

case class TransactionForeachPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  recoveryMode: RecoveryMode,
  statusVariableOpt: Option[String],
  retryPolicy: TransactionRetryPolicy
)(
  val id: Id = Id.INVALID_ID
) extends AbstractTransactionForeachPipe(source, inner, batchSize, recoveryMode, retryPolicy) {

  override protected def withStatus(
    output: ClosingIterator[CypherRow],
    status: TransactionStatus
  ): ClosingIterator[CypherRow] = statusVariableOpt match {
    case Some(statusVariable) => output.withVariable(statusVariable, toStatusMap(status))
    case _                    => output
  }
}

object TransactionForeachPipe {
  private val notRunStatus = statusMap(None, started = false, committed = false, None)

  def toStatusMap(status: TransactionStatus): AnyValue = {
    status match {
      case Commit(transactionId, _, _) =>
        statusMap(Some(transactionId), started = true, committed = true, None)
      case Rollback(transactionId, failure, _, _) =>
        statusMap(Some(transactionId), started = true, committed = false, Some(failure.getMessage))
      case NotRun              => notRunStatus
      case NonRecoverableError =>
        // Non-recoverable exception types are not expected to be caught and handled at this level
        throw new IllegalArgumentException("Unexpected handling of non-recoverable error status")
    }
  }

  private def statusMap(txId: Option[String], started: Boolean, committed: Boolean, error: Option[String]): MapValue = {
    val builder = new MapValueBuilder(4)
    builder.add("transactionId", txId.map(stringValue).getOrElse(NO_VALUE))
    builder.add("started", booleanValue(started))
    builder.add("committed", booleanValue(committed))
    builder.add("errorMessage", error.map(stringValue).getOrElse(NO_VALUE))
    builder.build()
  }
}
