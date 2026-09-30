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
package org.neo4j.cypher.internal.runtime.memory

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.HeapEstimatorCacheConfig
import org.neo4j.memory.LocalMemoryTracker

/**
 * Unit test of the per-lane inner-transaction provider used by the parallel runtime for
 * CALL IN TRANSACTIONS. The tests run on a plain thread (not a Cypher worker thread), so the
 * outer fallback of the provider resolves to the initialization memory tracker, standing in for
 * the outer transaction attribution that worker threads get through their execution context.
 */
class ParallelTransactionBoundMemoryTrackerForOperatorProviderTest extends CypherFunSuite {

  private def newProviderWithInitTracker()
    : (ParallelTransactionBoundMemoryTrackerForOperatorProvider, LocalMemoryTracker) = {
    val provider = new ParallelTransactionBoundMemoryTrackerForOperatorProvider(HeapEstimatorCacheConfig.DEFAULT)
    val initTracker = new LocalMemoryTracker()
    provider.setInitializationMemoryTracker(initTracker)
    (provider, initTracker)
  }

  test("should attribute to the outer fallback when no inner transaction is bound") {
    val (provider, initTracker) = newProviderWithInitTracker()
    val operatorTracker = provider.memoryTrackerForOperator(0)

    operatorTracker.allocateHeap(100)
    initTracker.estimatedHeapMemory() shouldBe 100

    operatorTracker.releaseHeap(100)
    initTracker.estimatedHeapMemory() shouldBe 0
  }

  test("should attribute to the inner transaction tracker between rebind and closeInner") {
    val (provider, initTracker) = newProviderWithInitTracker()
    val operatorTracker = provider.memoryTrackerForOperator(0)
    val innerTxTracker = new LocalMemoryTracker()

    provider.rebind(innerTxTracker)
    operatorTracker.allocateHeap(100)
    innerTxTracker.estimatedHeapMemory() shouldBe 100
    initTracker.estimatedHeapMemory() shouldBe 0

    operatorTracker.releaseHeap(100)
    innerTxTracker.estimatedHeapMemory() shouldBe 0

    provider.closeInner()
    operatorTracker.allocateHeap(50)
    initTracker.estimatedHeapMemory() shouldBe 50
    innerTxTracker.estimatedHeapMemory() shouldBe 0
    operatorTracker.releaseHeap(50)
  }

  test("should attribute through all operator trackers of the scope to the same inner transaction") {
    val (provider, initTracker) = newProviderWithInitTracker()
    val operatorTracker1 = provider.memoryTrackerForOperator(1)
    val operatorTracker2 = provider.memoryTrackerForOperator(2)
    val innerTxTracker = new LocalMemoryTracker()

    provider.rebind(innerTxTracker)
    operatorTracker1.allocateHeap(100)
    operatorTracker2.allocateHeap(200)
    innerTxTracker.estimatedHeapMemory() shouldBe 300
    initTracker.estimatedHeapMemory() shouldBe 0

    operatorTracker1.releaseHeap(100)
    operatorTracker2.releaseHeap(200)
    provider.closeInner()
    innerTxTracker.estimatedHeapMemory() shouldBe 0
  }

  test("should release remaining inner memory at closeInner and carry it over to the next inner transaction") {
    val (provider, initTracker) = newProviderWithInitTracker()
    val operatorTracker = provider.memoryTrackerForOperator(0)
    val innerTxTracker1 = new LocalMemoryTracker()
    val innerTxTracker2 = new LocalMemoryTracker()

    provider.rebind(innerTxTracker1)
    operatorTracker.allocateHeap(100)

    // The batch commits with 100 bytes of tracked state still alive. closeInner releases it from
    // the first inner transaction and carries it over to the next inner scope.
    provider.closeInner()
    innerTxTracker1.estimatedHeapMemory() shouldBe 0

    provider.rebind(innerTxTracker2)
    innerTxTracker2.estimatedHeapMemory() shouldBe 100

    operatorTracker.releaseHeap(100)
    innerTxTracker2.estimatedHeapMemory() shouldBe 0
    provider.closeInner()
    initTracker.estimatedHeapMemory() shouldBe 0
  }
}
