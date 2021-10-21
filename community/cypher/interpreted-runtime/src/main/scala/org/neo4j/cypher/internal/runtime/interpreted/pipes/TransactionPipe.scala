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

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.EntityTransformer
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipe.CypherRowEntityTransformer
import org.neo4j.exceptions.InternalException

import scala.util.control.NonFatal

trait TransactionPipe {
  self: PipeWithSource =>

  val inner: Pipe

  def runInnerInTransaction(state: QueryState, batch: Seq[CypherRow]): Unit = {
    internalCreateResultsForOneBatch(state, batch, keepResults = false)
  }

  def runInnerInTransactionKeepResult(state: QueryState, batch: Seq[CypherRow]): Seq[CypherRow] = {
    internalCreateResultsForOneBatch(state, batch, keepResults = true)
  }

  private def internalCreateResultsForOneBatch(state: QueryState, batch: Seq[CypherRow], keepResults: Boolean): Seq[CypherRow] = {
    inNewTransaction(state) { stateWithNewTransaction =>
      val entityTransformer = new CypherRowEntityTransformer(stateWithNewTransaction.query.entityTransformer)

      val result: Seq[CypherRow] = batch.flatMap { outerRow =>
        // Row based caching relies on the transaction state to avoid stale reads (see AbstractCachedProperty.apply).
        // Since we do not share the transaction state we must clear the cached properties.
        outerRow.invalidateCachedProperties()

        val reboundRow = entityTransformer.copyWithEntityWrappingValuesRebound(outerRow)
        val innerState = stateWithNewTransaction.withInitialContext(reboundRow)
        val result = inner.createResults(innerState)
        if (!keepResults) {
          // empty result
          while (result.hasNext) {
            result.next()
          }
          ClosingIterator.empty
        } else {
          result
        }
      }

      val subqueryStatistics = stateWithNewTransaction.getStatistics
      state.query.addStatistics(subqueryStatistics)

      result
    }
  }

  def inNewTransaction(state: QueryState)(f: QueryState => Seq[CypherRow]): Seq[CypherRow] = {

    // Ensure that no write happens before a 'CALL { ... } IN TRANSACTIONS'
    if (state.query.transactionalContext.dataRead.transactionStateHasChanges) throw new InternalException("Expected transaction state to be empty when calling transactional subquery.")

    // beginTx()
    val stateWithNewTransaction = state.withNewTransaction()
    val innerTxContext = stateWithNewTransaction.query.transactionalContext

    try {
      val result = f(stateWithNewTransaction)

      // commitTx()
      innerTxContext.close()
      innerTxContext.commitTransaction()

      result
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

  def evaluateBatchSize(batchSize: Expression, state: QueryState): Long = {
    PipeHelper.evaluateStaticLongOrThrow(batchSize, _ > 0, state, "OF ... ROWS", " Must be a positive integer.")
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
}