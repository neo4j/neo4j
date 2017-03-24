/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_2.profiler

import org.neo4j.cypher.internal.compiler.v3_2.spi._

class ProfilingQueryContext(inner: QueryContext) extends DelegatingQueryContext(inner: QueryContext) {

  private var pageHits: Long = 0
  private var pageMisses: Long = 0

  override def transactionalContext: QueryTransactionalContext = new ProfilingTransactionalContext(inner.transactionalContext, inner)

  override def kernelStatisticProvider(): KernelStatisticProvider = new PeriodicCommitKernelStatisticProvider(inner.kernelStatisticProvider(), this)

  class ProfilingTransactionalContext(inner: QueryTransactionalContext, queryContext: QueryContext) extends DelegatingQueryTransactionalContext(inner) {
    override def commitAndRestartTx() {
      val statisticProvider = queryContext.kernelStatisticProvider()
      pageHits += statisticProvider.getPageCacheHits
      pageMisses += statisticProvider.getPageCacheMisses
      inner.commitAndRestartTx()
    }
  }

  private final class PeriodicCommitKernelStatisticProvider(inner: KernelStatisticProvider, profilingQueryContext: ProfilingQueryContext) extends KernelStatisticProvider {

    override def getPageCacheHits: Long = inner.getPageCacheHits + profilingQueryContext.pageHits

    override def getPageCacheMisses: Long = inner.getPageCacheMisses + profilingQueryContext.pageMisses
  }
}
