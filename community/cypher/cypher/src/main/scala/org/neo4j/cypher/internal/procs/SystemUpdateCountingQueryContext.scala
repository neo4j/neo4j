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
package org.neo4j.cypher.internal.procs

import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.planning.ExceptionTranslatingQueryContext
import org.neo4j.cypher.internal.procs.SystemUpdateCountingQueryContext.Counter
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.interpreted.CountingQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.DelegatingQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.virtual.MapValue

class SystemUpdateCountingQueryContext(override val inner: QueryContext, val contextVars: MapValue, val systemUpdates: Counter)
  extends DelegatingQueryContext(inner) with CountingQueryContext {

  val kernelTransactionalContext: TransactionalContext = inner match {
    case ctx: ExceptionTranslatingQueryContext => ctx.inner match {
      case tqc: TransactionBoundQueryContext => tqc.transactionalContext.tc
      case _ => throw new IllegalStateException("System updating query context can only contain a transaction bound query context")
    }
    case _ => throw new IllegalStateException("System updating query context can only contain an exception translating query context")
  }

  def getStatistics = QueryStatistics(systemUpdates = systemUpdates.count)

  override def getOptStatistics = Some(getStatistics)

  def withContextVars(newVars: MapValue): SystemUpdateCountingQueryContext = {
    if(newVars.size() > 0) new SystemUpdateCountingQueryContext(inner, contextVars.updatedWith(newVars), systemUpdates)
    else this
  }
}

object SystemUpdateCountingQueryContext {
  def from(ctx: QueryContext): SystemUpdateCountingQueryContext = ctx match {
    case c: SystemUpdateCountingQueryContext => c
    case c => new SystemUpdateCountingQueryContext(c, MapValue.EMPTY, new Counter())
  }

  class Counter {
    val counter: AtomicInteger = new AtomicInteger()

    def count: Int = counter.get()

    def increase(amount: Int = 1) {
      counter.addAndGet(amount)
    }
  }
}
