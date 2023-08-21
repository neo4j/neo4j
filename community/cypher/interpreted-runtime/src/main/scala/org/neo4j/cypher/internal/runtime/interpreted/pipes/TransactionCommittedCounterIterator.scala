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

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionCommittedCounterIterator.executeWithHandling
import org.neo4j.exceptions.Neo4jException
import org.neo4j.exceptions.StatusWrapCypherException
import org.neo4j.exceptions.StatusWrapCypherException.ExtraInformation.TRANSACTIONS_COMMITTED

class TransactionCommittedCounterIterator(inner: ClosingIterator[CypherRow], queryState: QueryState)
    extends ClosingIterator[CypherRow] {
  override protected[this] def closeMore(): Unit = inner.close()

  def innerHasNext: Boolean = executeWithHandling(inner.hasNext, queryState)

  def next(): CypherRow = executeWithHandling(inner.next, queryState)
}

object TransactionCommittedCounterIterator {

  def wrap(f: () => ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] =
    executeWithHandling(
      {
        new TransactionCommittedCounterIterator(f(), state)
      },
      state
    )

  def executeWithHandling[T](f: => T, queryState: QueryState): T = {
    try {
      f
    } catch {
      case e: StatusWrapCypherException =>
        throw addTransactionInfoToException(e, queryState)
      case e: Neo4jException =>
        throw addTransactionInfoToException(new StatusWrapCypherException(e), queryState)
      case e: Throwable =>
        throw e
    }
  }

  private def addTransactionInfoToException[T](e: StatusWrapCypherException, queryState: QueryState) = {
    e.addExtraInfo(TRANSACTIONS_COMMITTED, transactionInfo(queryState))
  }

  private def transactionInfo(state: QueryState): String = {
    s"Transactions committed: ${state.getStatistics.transactionsCommitted}"
  }
}
