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
import org.neo4j.kernel.impl.util.collection.EagerBuffer
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

abstract class AbstractTransactionApplyPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  onErrorBehaviour: InTransactionsOnErrorBehaviour
) extends PipeWithSource(source) {

  protected def withStatus(output: ClosingIterator[CypherRow], status: TransactionStatus): ClosingIterator[CypherRow]
  protected def nullRows(value: EagerBuffer[CypherRow], state: QueryState): ClosingIterator[CypherRow]

  final override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val innerPipeInTx = TransactionPipeWrapper(onErrorBehaviour, id, inner, concurrentAccess = false)
    val batchSizeLong = evaluateBatchSize(batchSize, state)
    val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)

    input
      .eagerGrouped(batchSizeLong, memoryTracker)
      .flatMap { batch =>
        val innerResult = innerPipeInTx.createResults(state, batch, memoryTracker)
        val statistics = innerResult.status.queryStatistics
        if (statistics != null) {
          state.query.addStatistics(statistics)
        }
        val output = innerResult.committedResults match {
          case Some(result) =>
            batch.close()
            result.autoClosingIterator().asClosingIterator
          case _ => nullRows(batch, state)
        }

        withStatus(output, innerResult.status)
      }
  }
}

case class TransactionApplyPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  onErrorBehaviour: InTransactionsOnErrorBehaviour,
  nullableVariables: Set[String],
  statusVariableOpt: Option[String]
)(val id: Id = Id.INVALID_ID) extends AbstractTransactionApplyPipe(source, inner, batchSize, onErrorBehaviour) {

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
