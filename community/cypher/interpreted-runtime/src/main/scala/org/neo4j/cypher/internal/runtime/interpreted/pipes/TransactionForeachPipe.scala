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
import org.neo4j.cypher.internal.util.attribution.Id

import scala.util.control.NonFatal

case class TransactionForeachPipe(source: Pipe, inner: Pipe)
                                 (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  override protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    input.map { outerContext =>
      val innerState = state.withInitialContext(outerContext)

      inNewTransaction(innerState) { stateWithNewTransaction =>
        val ignoredResult = inner.createResults(stateWithNewTransaction)
        while (ignoredResult.hasNext) {
          ignoredResult.next()
        }
        val subqueryStatistics = stateWithNewTransaction.getStatistics
        state.query.addStatistics(subqueryStatistics)
      }

      outerContext
    }
  }

  def inNewTransaction(state: QueryState)(f: QueryState => Unit): Unit = {
    // beginTx()
    val stateWithNewTransaction = state.withNewTransaction()
    val innerTxContext = stateWithNewTransaction.query.transactionalContext

    try {
      f(stateWithNewTransaction)

      // commitTx()
      innerTxContext.close()
      innerTxContext.internalTransaction.commit()
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
