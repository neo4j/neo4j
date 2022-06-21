/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.EntityTransformer
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipe.CypherRowEntityTransformer
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipe.assertTransactionStateIsEmpty
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipe.commitTransactionWithStatistics
import org.neo4j.exceptions.InternalException
import org.neo4j.kernel.impl.util.collection.EagerBuffer

import scala.util.control.NonFatal

trait TransactionPipe {
  self: PipeWithSource =>

  val inner: Pipe

  /**
   * Evaluates inner pipe in a new transaction.
   * 
   * @param state query state
   * @param outerRows buffered outer rows, will not be closed by this method
   * @param f function to apply to inner rows
   */
  def createInnerResultsInNewTransaction(
    state: QueryState,
    outerRows: EagerBuffer[CypherRow]
  )(f: CypherRow => Unit): Unit = {

    // Ensure that no write happens before a 'CALL { ... } IN TRANSACTIONS'
    assertTransactionStateIsEmpty(state)

    // beginTx()
    val stateWithNewTransaction = state.withNewTransaction()
    val innerTxContext = stateWithNewTransaction.query.transactionalContext
    val entityTransformer = new CypherRowEntityTransformer(stateWithNewTransaction.query.entityTransformer)

    try {
      val batchIterator = outerRows.iterator()
      while (batchIterator.hasNext) {
        val outerRow = batchIterator.next()

        outerRow.invalidateCachedProperties()

        val reboundRow = entityTransformer.copyWithEntityWrappingValuesRebound(outerRow)
        val innerState = stateWithNewTransaction.withInitialContext(reboundRow)

        inner.createResults(innerState).foreach(f.apply) // Consume result before commit
      }

      state.query.addStatistics(stateWithNewTransaction.getStatistics)
      commitTransactionWithStatistics(innerTxContext, state)
    } catch {
      case NonFatal(e) =>
        try {
          innerTxContext.rollback()
        } catch {
          case NonFatal(rollbackException) =>
            e.addSuppressed(rollbackException)
        }
        throw e
    } finally {
      innerTxContext.close()
      stateWithNewTransaction.close()
    }
  }
}

object TransactionPipe {

  /**
   * Recursively finds entity wrappers and rebinds the entities to the current transaction
   */
  // TODO: Remove rebinding here, and transform wrappers to Reference:s
  // Currently, replacing e.g. NodeEntityWrappingNodeValue with NodeReference causes failures downstream.
  // We can for example end up in PathValueBuilder, which assumes that we have NodeValue and not NodeReference.
  // We can also still get entity values with transaction references streaming in and out of procedures.
  // Always copying the row should not be necessary. We could optimize this by first doing a dry-run to detect if anything actually needs to be rebound.
  class CypherRowEntityTransformer(entityTransformer: EntityTransformer) {

    def copyWithEntityWrappingValuesRebound(row: CypherRow): CypherRow =
      row.copyMapped(entityTransformer.rebindEntityWrappingValue)
  }

  def evaluateBatchSize(batchSize: Expression, state: QueryState): Long = {
    PipeHelper.evaluateStaticLongOrThrow(batchSize, _ > 0, state, "OF ... ROWS", " Must be a positive integer.")
  }

  def assertTransactionStateIsEmpty(state: QueryState): Unit = {
    if (state.query.transactionalContext.dataRead.transactionStateHasChanges)
      throw new InternalException("Expected transaction state to be empty when calling transactional subquery.")
  }

  private def commitTransactionWithStatistics(
    innerTxContext: QueryTransactionalContext,
    outerQueryState: QueryState
  ): Unit = {
    innerTxContext.close()
    innerTxContext.commitTransaction()

    val executionStatistics = QueryStatistics(transactionsCommitted = 1)
    outerQueryState.query.addStatistics(executionStatistics)
  }
}
