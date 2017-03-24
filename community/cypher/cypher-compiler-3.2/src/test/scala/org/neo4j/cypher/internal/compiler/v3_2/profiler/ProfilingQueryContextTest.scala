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

import org.mockito.Mockito
import org.neo4j.cypher.internal.compiler.v3_2.spi.{KernelStatisticProvider, QueryContext, QueryTransactionalContext}
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite

class ProfilingQueryContextTest extends CypherFunSuite {

  test("countPageCacheHitsAndMissesForAllRestartedTransactions") {
    val innerContext = mock[QueryContext]
    val statisticProvider: KernelStatisticProvider = new ConfiguredKernelStatisticProvider(Array(1, 2, 3, 4), Array(2, 0, 1, 9))
    Mockito.when(innerContext.kernelStatisticProvider()).thenReturn(statisticProvider)
    Mockito.when(innerContext.transactionalContext).thenReturn(mock[QueryTransactionalContext])

    val profilingContext = new ProfilingQueryContext(innerContext)
    val transactionalContext = profilingContext.transactionalContext
    transactionalContext.commitAndRestartTx()
    transactionalContext.commitAndRestartTx()
    transactionalContext.commitAndRestartTx()

    val provider = profilingContext.kernelStatisticProvider()
    provider.getPageCacheHits should equal (10)
    provider.getPageCacheMisses should equal (12)
  }

  private class ConfiguredKernelStatisticProvider(hits: Array[Long], misses: Array[Long]) extends KernelStatisticProvider {
    var iteration:Int = 0

    override def getPageCacheHits: Long = hits(iteration)

    override def getPageCacheMisses: Long = {
      val miss = misses(iteration)
      incrementIteration()
      miss
    }

    private def incrementIteration(): Unit = iteration += 1
  }
}
