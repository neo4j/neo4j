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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.interpreted.CountingQueryContext.Counter

class TransactionsCountingQueryContext(inner: QueryContext) extends DelegatingQueryContext(inner)
    with CountingQueryContext {
  private val transactionsCommitted = new Counter
  private val transactionsStarted = new Counter
  private val transactionsRolledBack = new Counter

  override def getTrackedStatistics: QueryStatistics = {
    QueryStatistics(
      transactionsCommitted = transactionsCommitted.count,
      transactionsStarted = transactionsStarted.count,
      transactionsRolledBack = transactionsRolledBack.count
    )
  }

  override def addStatistics(statistics: QueryStatistics): Unit = {
    transactionsCommitted.increase(statistics.transactionsCommitted)
    transactionsStarted.increase(statistics.transactionsStarted)
    transactionsRolledBack.increase(statistics.transactionsRolledBack)
    inner.addStatistics(statistics)
  }
}
