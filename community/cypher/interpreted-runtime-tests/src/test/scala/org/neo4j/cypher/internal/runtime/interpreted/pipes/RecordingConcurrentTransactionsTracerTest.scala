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

import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import java.util.function.LongSupplier

/**
 * Deterministic tests of the recording tracer and the query-level aggregate, driven by a scripted
 * clock. All trace points fire on the test thread; the worker start stamp lives on the batch, so
 * attempts may interleave freely as long as each batch's start precedes its finish.
 */
class RecordingConcurrentTransactionsTracerTest extends CypherFunSuite {

  private class ScriptedClock extends LongSupplier {
    var now: Long = 1L
    override def getAsLong: Long = now
  }

  private def newBatch(): TransactionBatch = new FreshTransactionBatch(null)

  test("records time-weighted queued batches and ready-to-start gaps") {
    val clock = new ScriptedClock
    val tracer = new RecordingConcurrentTransactionsTracer(clock)
    val batchA = newBatch()
    val batchB = newBatch()

    clock.now = 1000L
    tracer.onProducerBatchReady(batchA)
    clock.now = 2000L
    tracer.onProducerBatchReady(batchB)
    // Queued count 2 during [2000, 3000), 1 during [1000, 2000)
    clock.now = 3000L
    tracer.onWorkerBatchStarted(batchA)
    clock.now = 4000L
    tracer.onWorkerBatchFinished(batchA)
    // Queued count 1 during [3000, 5000)
    clock.now = 5000L
    tracer.onWorkerBatchStarted(batchB)
    clock.now = 6000L
    tracer.onWorkerBatchFinished(batchB)

    val profile = tracer.snapshot()
    // Integral: 1*1000 + 2*1000 + 1*2000 = 5000 over the span [1000, 6000)
    profile.avgQueuedBatches shouldBe 1.0
    profile.peakQueuedBatches shouldBe 2L
    profile.batchesQueued shouldBe 2L
    profile.batchesStarted shouldBe 2L
    // Worker time 2 * 1000 over the span [3000, 6000)
    profile.avgConcurrency shouldBe (2000.0 / 3000.0)
    profile.peakConcurrency shouldBe 1L
    // Gaps: A waited [1000, 3000), B waited [2000, 5000)
    profile.peakStartGapNanos shouldBe 3000L
    profile.avgStartGapNanos shouldBe 2500.0
  }

  test("records inner transaction counts, retries and the concurrency limit") {
    val clock = new ScriptedClock
    val tracer = new RecordingConcurrentTransactionsTracer(clock)
    val batch = newBatch()

    tracer.onProducerInit(8L)
    clock.now = 1000L
    tracer.onProducerBatchReady(batch)
    clock.now = 2000L
    tracer.onWorkerBatchStarted(batch)
    tracer.onWorkerInnerTransactionStarted()
    tracer.onWorkerInnerTransactionCommitted()
    clock.now = 3000L
    tracer.onWorkerBatchFinished(batch)
    tracer.onProducerBatchRetried(batch)

    val profile = tracer.snapshot()
    profile.concurrencyLimit shouldBe 8L
    profile.innerTransactionsStarted shouldBe 1L
    profile.innerTransactionsCommitted shouldBe 1L
    profile.retries shouldBe 1L
  }

  test("a retry attempt is counted and timed as a fresh attempt") {
    val clock = new ScriptedClock
    val tracer = new RecordingConcurrentTransactionsTracer(clock)
    val batch = newBatch()

    clock.now = 1000L
    tracer.onProducerBatchReady(batch)
    clock.now = 2000L
    tracer.onWorkerBatchStarted(batch)
    clock.now = 3000L
    tracer.onWorkerBatchFinished(batch)
    // The retry attempt reuses the batch object; the ready stamp is overwritten per attempt
    clock.now = 4000L
    tracer.onProducerBatchReady(batch)
    clock.now = 5000L
    tracer.onWorkerBatchStarted(batch)
    clock.now = 6000L
    tracer.onWorkerBatchFinished(batch)

    val profile = tracer.snapshot()
    profile.batchesQueued shouldBe 2L
    profile.batchesStarted shouldBe 2L
    profile.peakStartGapNanos shouldBe 1000L
    profile.avgStartGapNanos shouldBe 1000.0
    // Integral: 1*1000 (first wait) + 1*1000 (retry wait) over the span [1000, 6000)
    profile.avgQueuedBatches shouldBe (2000.0 / 5000.0)
  }

  test("queued time of batches abandoned before starting is counted exactly") {
    val clock = new ScriptedClock
    val tracer = new RecordingConcurrentTransactionsTracer(clock)
    val batchA = newBatch()
    val batchB = newBatch()

    clock.now = 1000L
    tracer.onProducerBatchReady(batchA)
    tracer.onProducerBatchReady(batchB)
    clock.now = 2000L
    tracer.onWorkerBatchStarted(batchA)
    clock.now = 3000L
    tracer.onWorkerBatchFinished(batchA)
    // batchB was made ready but never started: the query ended early (for example a LIMIT above
    // the operator). Its queued time must be accounted up to the snapshot.
    clock.now = 5000L
    val profile = tracer.snapshot()

    profile.batchesQueued shouldBe 2L
    profile.batchesStarted shouldBe 1L
    // Integral: 2*1000 during [1000, 2000) + 1*3000 during [2000, 5000) = 5000 over span [1000, 5000)
    profile.avgQueuedBatches shouldBe (5000.0 / 4000.0)
    profile.peakQueuedBatches shouldBe 2L
  }

  test("aggregate does not inflate average concurrency for sequential operators") {
    val clock = new ScriptedClock
    val tracers = new RecordingQueryTracers(clock)
    val op1 = tracers.concurrentTransactions(Id(1))
    val op2 = tracers.concurrentTransactions(Id(2))
    val batch1 = newBatch()
    val batch2 = newBatch()

    // Operator 1 fully busy during [1000, 2000)
    clock.now = 1000L
    op1.onProducerBatchReady(batch1)
    op1.onWorkerBatchStarted(batch1)
    clock.now = 2000L
    op1.onWorkerBatchFinished(batch1)
    // Operator 2 fully busy during [2000, 3000)
    op2.onProducerBatchReady(batch2)
    op2.onWorkerBatchStarted(batch2)
    clock.now = 3000L
    op2.onWorkerBatchFinished(batch2)

    op1.snapshot().avgConcurrency shouldBe 1.0
    op2.snapshot().avgConcurrency shouldBe 1.0

    val aggregate = tracers.aggregateSnapshot()
    // Two sequential operators each at avg 1.0 must not sum to 2.0: total worker time 2000 over the
    // union span [1000, 3000)
    aggregate.avgConcurrency shouldBe 1.0
    aggregate.peakConcurrency shouldBe 1L
  }

  test("aggregate adds up worker time for overlapping operators") {
    val clock = new ScriptedClock
    val tracers = new RecordingQueryTracers(clock)
    val op1 = tracers.concurrentTransactions(Id(1))
    val op2 = tracers.concurrentTransactions(Id(2))
    val batch1 = newBatch()
    val batch2 = newBatch()

    // Both operators fully busy during [1000, 2000)
    clock.now = 1000L
    op1.onProducerBatchReady(batch1)
    op1.onWorkerBatchStarted(batch1)
    op2.onProducerBatchReady(batch2)
    op2.onWorkerBatchStarted(batch2)
    clock.now = 2000L
    op1.onWorkerBatchFinished(batch1)
    op2.onWorkerBatchFinished(batch2)

    val aggregate = tracers.aggregateSnapshot()
    aggregate.avgConcurrency shouldBe 2.0
  }

  test("aggregate weights queued averages by time instead of taking the maximum") {
    val clock = new ScriptedClock
    val tracers = new RecordingQueryTracers(clock)
    val op1 = tracers.concurrentTransactions(Id(1))
    val op2 = tracers.concurrentTransactions(Id(2))
    val batch1a = newBatch()
    val batch1b = newBatch()
    val batch2 = newBatch()

    // Operator 1: two batches queued during [1000, 2000), both run during [2000, 3000)
    clock.now = 1000L
    op1.onProducerBatchReady(batch1a)
    op1.onProducerBatchReady(batch1b)
    clock.now = 2000L
    op1.onWorkerBatchStarted(batch1a)
    op1.onWorkerBatchFinished(batch1a)
    op1.onWorkerBatchStarted(batch1b)
    clock.now = 3000L
    op1.onWorkerBatchFinished(batch1b)
    // Operator 2: never waits, runs during [3000, 4000)
    op2.onProducerBatchReady(batch2)
    op2.onWorkerBatchStarted(batch2)
    clock.now = 4000L
    op2.onWorkerBatchFinished(batch2)

    op1.snapshot().avgQueuedBatches shouldBe 1.0
    op2.snapshot().avgQueuedBatches shouldBe 0.0

    val aggregate = tracers.aggregateSnapshot()
    // Integral 2000 over the union span [1000, 4000), not max(1.0, 0.0)
    aggregate.avgQueuedBatches shouldBe (2000.0 / 3000.0)
    aggregate.peakQueuedBatches shouldBe 2L
  }

  test("aggregate takes the maximum concurrency limit across operators") {
    val tracers = new RecordingQueryTracers(new ScriptedClock)
    val op1 = tracers.concurrentTransactions(Id(1))
    val op2 = tracers.concurrentTransactions(Id(2))

    op1.onProducerInit(8L)
    op2.onProducerInit(4L)

    tracers.aggregateSnapshot().concurrencyLimit shouldBe 8L
  }

  test("an operator without any batches does not corrupt the aggregate") {
    val clock = new ScriptedClock
    val tracers = new RecordingQueryTracers(clock)
    val op1 = tracers.concurrentTransactions(Id(1))
    // Created but never fired: for example a CONCURRENT TRANSACTIONS operator with empty input
    tracers.concurrentTransactions(Id(2))
    val batch = newBatch()

    clock.now = 1000L
    op1.onProducerBatchReady(batch)
    op1.onWorkerBatchStarted(batch)
    clock.now = 2000L
    op1.onWorkerBatchFinished(batch)

    val aggregate = tracers.aggregateSnapshot()
    aggregate.avgConcurrency shouldBe 1.0
    aggregate.batchesQueued shouldBe 1L
  }

  test("an empty recording tracer snapshots to all zeros") {
    val profile = new RecordingConcurrentTransactionsTracer(new ScriptedClock).snapshot()
    profile.peakConcurrency shouldBe 0L
    profile.avgConcurrency shouldBe 0.0
    profile.peakQueuedBatches shouldBe 0L
    profile.avgQueuedBatches shouldBe 0.0
    profile.peakStartGapNanos shouldBe 0L
    profile.avgStartGapNanos shouldBe 0.0
    profile.concurrencyLimit shouldBe 0L
    profile.batchesQueued shouldBe 0L
    profile.batchesStarted shouldBe 0L
  }
}
