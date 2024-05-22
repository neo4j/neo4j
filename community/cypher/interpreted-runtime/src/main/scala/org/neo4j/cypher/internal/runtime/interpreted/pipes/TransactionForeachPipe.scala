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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.evaluateBatchSize
import org.neo4j.cypher.internal.util.attribution.Id
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
  onErrorBehaviour: InTransactionsOnErrorBehaviour
) extends PipeWithSource(source) {

  protected def withStatus(output: ClosingIterator[CypherRow], status: TransactionStatus): ClosingIterator[CypherRow]

  final override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val innerInTx = TransactionPipeWrapper(onErrorBehaviour, inner, concurrentAccess = false)
    val batchSizeLong = evaluateBatchSize(batchSize, state)
    val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)

    input
      .eagerGrouped(batchSizeLong, memoryTracker)
      .flatMap { batch =>
        val status = innerInTx.consume(state, batch)
        val statistics = status.queryStatistics
        if (statistics != null) {
          state.query.addStatistics(statistics)
        }
        val output = batch.autoClosingIterator().asClosingIterator
        withStatus(output, status)
      }
  }
}

case class TransactionForeachPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  onErrorBehaviour: InTransactionsOnErrorBehaviour,
  statusVariableOpt: Option[String]
)(
  val id: Id = Id.INVALID_ID
) extends AbstractTransactionForeachPipe(source, inner, batchSize, onErrorBehaviour) {

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
      case NotRun => notRunStatus
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
