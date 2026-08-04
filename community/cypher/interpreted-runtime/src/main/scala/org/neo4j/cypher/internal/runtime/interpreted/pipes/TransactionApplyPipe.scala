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
import org.neo4j.values.storable.Values

abstract class AbstractTransactionApplyPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  recoveryMode: RecoveryMode,
  retryPolicy: TransactionRetryPolicy
) extends AbstractSerialTransactionsPipe(source, inner, batchSize, recoveryMode, retryPolicy) {

  protected def withStatus(output: ClosingIterator[CypherRow], status: TransactionStatus): ClosingIterator[CypherRow]
  protected def nullRows(value: EagerBuffer[CypherRow], state: QueryState): ClosingIterator[CypherRow]

  override protected def produceOutput(
    eagerBuffer: EagerBuffer[CypherRow],
    result: TransactionResult,
    batch: TransactionBatch,
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val output = result.committedResults match {
      case Some(result) =>
        batch.close()
        result.autoClosingIterator().asClosingIterator
      case _ => nullRows(batch.rows, state)
    }

    withStatus(output, result.status)
  }

  override protected def getResult(
    innerPipeInTx: TransactionPipeWrapper,
    state: QueryState,
    batch: TransactionBatch,
    memoryTracker: MemoryTracker
  ): TransactionResult =
    innerPipeInTx.createResults(state, batch, memoryTracker)
}

case class TransactionApplyPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  recoveryMode: RecoveryMode,
  nullableVariables: Set[String],
  statusVariableOpt: Option[String],
  retryPolicy: TransactionRetryPolicy
)(val id: Id = Id.INVALID_ID)
    extends AbstractTransactionApplyPipe(source, inner, batchSize, recoveryMode, retryPolicy) {

  private lazy val nullEntries: Seq[(String, AnyValue)] = {
    nullableVariables.toIndexedSeq.map(name => name -> Values.NO_VALUE)
  }

  override protected def withStatus(
    output: ClosingIterator[CypherRow],
    status: TransactionStatus
  ): ClosingIterator[CypherRow] = statusVariableOpt match {
    case Some(statusVariable) => output.withVariable(statusVariable, toStatusMap(status))
    case _                    => output
  }

  override protected def nullRows(lhs: EagerBuffer[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    lhs.autoClosingIterator().asClosingIterator.map { row =>
      val nullRow = state.newRowWithArgument(rowFactory)
      nullRow.mergeWith(row, state.query)
      nullRow.set(nullEntries)
      nullRow
    }
  }
}
