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
import org.neo4j.cypher.result.ConcurrentTransactionsProfile

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.function.LongSupplier

/**
 * Per-operator tracer for a CONCURRENT TRANSACTIONS operator (TransactionApply / TransactionForeach).
 *
 * Trace-point names are prefixed with the side/thread they fire on so it is always clear from the
 * call site which thread is calling: `onProducer*` fires on the main (producer) thread inside the
 * pipe iterator; `onWorker*` fires on a TransactionWorker thread.
 *
 * The trace points are defined on the batch attempt lifecycle (queued, then in flight) rather than
 * on any particular scheduling data structure, so a different runtime implementation of CONCURRENT
 * TRANSACTIONS can drive the same tracer.
 *
 * Implementations MUST be thread-safe and MUST NOT throw. The default [[ConcurrentTransactionsTracer.NULL]]
 * has zero overhead, so a query that is not being observed pays nothing.
 */
trait ConcurrentTransactionsTracer {

  /** Main thread: the operator started executing with the given effective maximum concurrency. */
  def onProducerInit(concurrencyLimit: Long): Unit

  /**
   * Main thread: a batch attempt is fully formed (accepts no more rows) and awaits execution
   * (queued), whether held back by conflict-avoiding scheduling, staged, or waiting for a free
   * worker. Fires exactly once per attempt, before the attempt reaches a worker: for a first
   * attempt when batch formation completes, for a retry attempt when its backoff has elapsed and
   * it is polled for re-execution.
   */
  def onProducerBatchReady(batch: TransactionBatch): Unit

  /** Worker thread: a worker has started processing a batch (the attempt leaves the queued state). */
  def onWorkerBatchStarted(batch: TransactionBatch): Unit

  /** Worker thread: the inner transaction for the current batch has begun successfully. */
  def onWorkerInnerTransactionStarted(): Unit

  /** Worker thread: the inner transaction for the current batch has committed successfully. */
  def onWorkerInnerTransactionCommitted(): Unit

  /** Worker thread: a worker has finished processing a batch. */
  def onWorkerBatchFinished(batch: TransactionBatch): Unit

  /** Main thread: a batch was scheduled for retry (ON ERROR RETRY) after a recoverable failure. */
  def onProducerBatchRetried(batch: TransactionBatch): Unit

  /** Main thread: read the accumulated stats. Returns `null` for the noop tracer. */
  def snapshot(): ConcurrentTransactionsProfile
}

object ConcurrentTransactionsTracer {

  val NULL: ConcurrentTransactionsTracer = new ConcurrentTransactionsTracer {
    override def onProducerInit(concurrencyLimit: Long): Unit = ()
    override def onProducerBatchReady(batch: TransactionBatch): Unit = ()
    override def onWorkerBatchStarted(batch: TransactionBatch): Unit = ()
    override def onWorkerInnerTransactionStarted(): Unit = ()
    override def onWorkerInnerTransactionCommitted(): Unit = ()
    override def onWorkerBatchFinished(batch: TransactionBatch): Unit = ()
    override def onProducerBatchRetried(batch: TransactionBatch): Unit = ()
    override def snapshot(): ConcurrentTransactionsProfile = null
  }

  private[pipes] val SYSTEM_CLOCK: LongSupplier = () => System.nanoTime()

  /**
   * Raw accumulator values of one recording tracer. Query-level aggregation happens on these (sums,
   * mins and maxes merge exactly) rather than on derived averages, which cannot be combined without
   * their weights.
   */
  private[pipes] case class RawStats(
    peakConcurrency: Long,
    totalWorkerNanos: Long,
    firstStartNanos: Long,
    lastEndNanos: Long,
    peakQueuedBatches: Long,
    queuedIntegralNanos: Long,
    firstReadyNanos: Long,
    lastQueuedEventNanos: Long,
    peakStartGapNanos: Long,
    totalStartGapNanos: Long,
    startGapSamples: Long,
    concurrencyLimit: Long,
    batchesQueued: Long,
    batchesStarted: Long,
    innerTransactionsStarted: Long,
    innerTransactionsCommitted: Long,
    retries: Long
  ) {

    def merge(other: RawStats): RawStats = RawStats(
      peakConcurrency = Math.max(peakConcurrency, other.peakConcurrency),
      totalWorkerNanos = totalWorkerNanos + other.totalWorkerNanos,
      firstStartNanos = Math.min(firstStartNanos, other.firstStartNanos),
      lastEndNanos = Math.max(lastEndNanos, other.lastEndNanos),
      peakQueuedBatches = Math.max(peakQueuedBatches, other.peakQueuedBatches),
      queuedIntegralNanos = queuedIntegralNanos + other.queuedIntegralNanos,
      firstReadyNanos = minSet(firstReadyNanos, other.firstReadyNanos),
      lastQueuedEventNanos = Math.max(lastQueuedEventNanos, other.lastQueuedEventNanos),
      peakStartGapNanos = Math.max(peakStartGapNanos, other.peakStartGapNanos),
      totalStartGapNanos = totalStartGapNanos + other.totalStartGapNanos,
      startGapSamples = startGapSamples + other.startGapSamples,
      concurrencyLimit = Math.max(concurrencyLimit, other.concurrencyLimit),
      batchesQueued = batchesQueued + other.batchesQueued,
      batchesStarted = batchesStarted + other.batchesStarted,
      innerTransactionsStarted = innerTransactionsStarted + other.innerTransactionsStarted,
      innerTransactionsCommitted = innerTransactionsCommitted + other.innerTransactionsCommitted,
      retries = retries + other.retries
    )

    // 0 means unset for firstReadyNanos, so the minimum only considers set values
    private def minSet(a: Long, b: Long): Long =
      if (a == 0L) b else if (b == 0L) a else Math.min(a, b)

    def toProfile: ConcurrentTransactionsProfile = {
      val concurrencySpan = lastEndNanos - firstStartNanos
      val avgConcurrency = if (concurrencySpan <= 0L) 0.0 else totalWorkerNanos.toDouble / concurrencySpan

      // The queued span runs from the first queued event to the last lifecycle event. When the
      // queued count reached zero (every attempt started), extending the span to the last worker
      // end adds nothing to the numerator and the average is exact. When attempts never started
      // (abandoned on early termination), raw advances the integral to the snapshot time, so their
      // queued time contributes exactly as well.
      val queuedSpanEnd = Math.max(lastEndNanos, lastQueuedEventNanos)
      val avgQueuedBatches =
        if (firstReadyNanos == 0L || queuedSpanEnd <= firstReadyNanos) 0.0
        else queuedIntegralNanos.toDouble / (queuedSpanEnd - firstReadyNanos)

      val avgStartGapNanos = if (startGapSamples <= 0L) 0.0 else totalStartGapNanos.toDouble / startGapSamples

      new ConcurrentTransactionsProfile(
        peakConcurrency,
        avgConcurrency,
        peakQueuedBatches,
        avgQueuedBatches,
        peakStartGapNanos,
        avgStartGapNanos,
        concurrencyLimit,
        batchesQueued,
        batchesStarted,
        innerTransactionsStarted,
        innerTransactionsCommitted,
        retries
      )
    }
  }
}

/**
 * Thread-safe recording tracer. Records max in-flight workers (peak concurrency), sustained average
 * concurrency (total worker-time / wall-clock span), the number of queued batch attempts as a
 * time-weighted count, and the per-attempt queued-to-start gap, plus inner-transaction begin/commit
 * counts and retries.
 *
 * The ready and worker-start timestamps are stashed on the batch itself ([[TransactionBatch.readyNanos]]
 * and [[TransactionBatch.startNanos]]) and read on the worker thread; the executor submit establishes
 * the necessary happens-before, so no volatile is needed. Per-attempt counting: a retried batch is a
 * fresh attempt and is counted again.
 *
 * The queued count changes on the producer thread (ready) and on worker threads (start), so its
 * time-weighted integral is maintained under a small lock; two short critical sections per attempt,
 * negligible next to beginning and committing an inner transaction.
 *
 * @param clock nanoTime source, injectable for deterministic tests
 */
class RecordingConcurrentTransactionsTracer(clock: LongSupplier = ConcurrentTransactionsTracer.SYSTEM_CLOCK)
    extends ConcurrentTransactionsTracer {
  private[this] val started = new AtomicInteger(0)
  private[this] val peak = new AtomicInteger(0)
  private[this] val inFlight = new AtomicInteger(0)

  // 0 is used as the unset sentinel for nanoTime stamps here and on the batch scratch fields. The
  // JDK permits nanoTime to be zero or negative, in which case the time-derived stats silently
  // degrade toward zero while counts and peaks stay correct. Mainstream JVMs return positive
  // since-boot values, so this is accepted.
  private[this] val firstStartNanos = new AtomicLong(Long.MaxValue)
  private[this] val lastEndNanos = new AtomicLong(0L)
  private[this] val totalWorkerNanos = new AtomicLong(0L)

  @volatile private[this] var concurrencyLimit: Long = 0L

  // Queued-batch accounting, guarded by queuedLock: the count transitions on both the producer
  // thread (+1 at ready) and worker threads (-1 at start), and the time-weighted integral must be
  // advanced atomically with each transition.
  private[this] val queuedLock = new Object
  private[this] var queuedCount: Int = 0
  private[this] var peakQueued: Int = 0
  private[this] var queuedIntegralNanos: Long = 0L
  private[this] var firstReadyNanos: Long = 0L
  private[this] var lastQueuedEventNanos: Long = 0L
  private[this] var batchesQueued: Long = 0L

  private[this] val peakStartGapNanos = new AtomicLong(0L)
  private[this] val totalStartGapNanos = new AtomicLong(0L)
  private[this] val startGapSamples = new AtomicLong(0L)

  private[this] val innerTxStarted = new AtomicLong(0L)
  private[this] val innerTxCommitted = new AtomicLong(0L)
  private[this] val retries = new AtomicLong(0L)

  override def onProducerInit(concurrencyLimit: Long): Unit = {
    this.concurrencyLimit = concurrencyLimit
  }

  override def onProducerBatchReady(batch: TransactionBatch): Unit = {
    val now = clock.getAsLong
    batch.readyNanos = now
    queuedLock.synchronized {
      if (firstReadyNanos == 0L) firstReadyNanos = now
      advanceQueuedIntegral(now)
      queuedCount += 1
      if (queuedCount > peakQueued) peakQueued = queuedCount
      batchesQueued += 1
    }
  }

  override def onWorkerBatchStarted(batch: TransactionBatch): Unit = {
    val now = clock.getAsLong
    batch.startNanos = now
    started.incrementAndGet()
    var fs = firstStartNanos.get()
    while (now < fs && !firstStartNanos.compareAndSet(fs, now)) fs = firstStartNanos.get()

    val current = inFlight.incrementAndGet()
    var pPrev = peak.get()
    while (current > pPrev && !peak.compareAndSet(pPrev, current)) pPrev = peak.get()

    queuedLock.synchronized {
      advanceQueuedIntegral(now)
      if (queuedCount > 0) {
        queuedCount -= 1
      }
    }

    val ready = batch.readyNanos
    if (ready > 0L) {
      val gap = now - ready
      if (gap >= 0L) {
        totalStartGapNanos.addAndGet(gap)
        var gPrev = peakStartGapNanos.get()
        while (gap > gPrev && !peakStartGapNanos.compareAndSet(gPrev, gap)) gPrev = peakStartGapNanos.get()
        startGapSamples.incrementAndGet()
      }
    }
  }

  override def onWorkerInnerTransactionStarted(): Unit = innerTxStarted.incrementAndGet()

  override def onWorkerInnerTransactionCommitted(): Unit = innerTxCommitted.incrementAndGet()

  override def onProducerBatchRetried(batch: TransactionBatch): Unit = retries.incrementAndGet()

  override def onWorkerBatchFinished(batch: TransactionBatch): Unit = {
    val now = clock.getAsLong
    val start = batch.startNanos
    if (start != 0L) {
      totalWorkerNanos.addAndGet(now - start)
      batch.startNanos = 0L
    }
    var le = lastEndNanos.get()
    while (now > le && !lastEndNanos.compareAndSet(le, now)) le = lastEndNanos.get()
    inFlight.decrementAndGet()
  }

  // Must be called while holding queuedLock. Never moves the transition marker backwards, so a
  // slightly out-of-order timestamp from another thread cannot double-count an interval.
  private def advanceQueuedIntegral(now: Long): Unit = {
    if (lastQueuedEventNanos == 0L) {
      lastQueuedEventNanos = now
    } else if (now > lastQueuedEventNanos) {
      queuedIntegralNanos += (now - lastQueuedEventNanos) * queuedCount
      lastQueuedEventNanos = now
    }
  }

  private[pipes] def raw: ConcurrentTransactionsTracer.RawStats = {
    val (rawPeakQueued, rawQueuedIntegral, rawFirstReady, rawLastQueuedEvent, rawBatchesQueued) =
      queuedLock.synchronized {
        // Attempts still queued at snapshot time (abandoned on early termination) contribute their
        // queued time up to this point, keeping the time-weighted average exact for them too
        if (queuedCount > 0) {
          advanceQueuedIntegral(clock.getAsLong)
        }
        (peakQueued.toLong, queuedIntegralNanos, firstReadyNanos, lastQueuedEventNanos, batchesQueued)
      }
    ConcurrentTransactionsTracer.RawStats(
      peakConcurrency = peak.get().toLong,
      totalWorkerNanos = totalWorkerNanos.get(),
      firstStartNanos = firstStartNanos.get(),
      lastEndNanos = lastEndNanos.get(),
      peakQueuedBatches = rawPeakQueued,
      queuedIntegralNanos = rawQueuedIntegral,
      firstReadyNanos = rawFirstReady,
      lastQueuedEventNanos = rawLastQueuedEvent,
      peakStartGapNanos = peakStartGapNanos.get(),
      totalStartGapNanos = totalStartGapNanos.get(),
      startGapSamples = startGapSamples.get(),
      concurrencyLimit = concurrencyLimit,
      batchesQueued = rawBatchesQueued,
      batchesStarted = started.get().toLong,
      innerTransactionsStarted = innerTxStarted.get(),
      innerTransactionsCommitted = innerTxCommitted.get(),
      retries = retries.get()
    )
  }

  override def snapshot(): ConcurrentTransactionsProfile = raw.toProfile
}

/**
 * Generic per-query container of runtime tracers, held by [[QueryState]]. Adding a future tracer is a
 * new method here and does not change the [[QueryState]] constructor signature.
 */
trait QueryTracers {

  /**
   * Returns the (cached) tracer for the given CONCURRENT TRANSACTIONS operator. The same instance is
   * returned for the same operator id, so the producer iterator, the workers and the commit path all
   * share one tracer per operator.
   */
  def concurrentTransactions(operatorId: Id): ConcurrentTransactionsTracer

  /**
   * Aggregate snapshot across all recorded CONCURRENT TRANSACTIONS operators, or `null` if nothing was
   * recorded (the noop container, or a recording container for a query without such an operator). Read
   * when snapshotting the query statistics.
   *
   * Mid-execution reads (for example a monitoring thread sampling an ExecutingQuery) are thread-safe
   * but see no in-progress worker time: a batch contributes to average concurrency only once it
   * finishes, so a sampler can read avgConcurrency near zero during a long-running batch even with
   * all workers busy. If SHOW TRANSACTIONS surfaces these fields, that limitation must be addressed
   * or documented there.
   */
  def aggregateSnapshot(): ConcurrentTransactionsProfile
}

object QueryTracers {

  val NULL: QueryTracers = new QueryTracers {
    override def concurrentTransactions(operatorId: Id): ConcurrentTransactionsTracer =
      ConcurrentTransactionsTracer.NULL
    override def aggregateSnapshot(): ConcurrentTransactionsProfile = null
  }
}

/**
 * Recording container that lazily creates one [[RecordingConcurrentTransactionsTracer]] per operator
 * id. The per-id map is touched once per operator (at pipe init / first hook), not per batch.
 *
 * @param clock nanoTime source handed to created tracers, injectable for deterministic tests
 */
class RecordingQueryTracers(clock: LongSupplier = ConcurrentTransactionsTracer.SYSTEM_CLOCK) extends QueryTracers {
  private[this] val tracers = new ConcurrentHashMap[Id, RecordingConcurrentTransactionsTracer]()

  // Hoisted so per-batch lookups (the commit path) do not allocate a capturing lambda per call
  private[this] val newTracer: java.util.function.Function[Id, RecordingConcurrentTransactionsTracer] =
    _ => new RecordingConcurrentTransactionsTracer(clock)

  override def concurrentTransactions(operatorId: Id): ConcurrentTransactionsTracer =
    tracers.computeIfAbsent(operatorId, newTracer)

  /**
   * Aggregate across all recorded operators, merged from the raw accumulators (sums, mins and maxes)
   * rather than from derived averages. The query-level averages are time-weighted over the whole
   * envelope from the earliest start to the latest lifecycle event, not the union of the operators'
   * active spans, so with more than one CONCURRENT TRANSACTIONS operator an idle gap between
   * operators dilutes the average and overlapping operators can push average concurrency above the
   * limit. Peaks and the concurrency limit are the maximum within any single operator: a true
   * cross-operator peak is not reconstructible from per-operator maxima taken at different instants,
   * and plans with more than one CONCURRENT TRANSACTIONS operator are rare enough that reconstructing
   * an exact cross-operator average is not worth its cost.
   */
  override def aggregateSnapshot(): ConcurrentTransactionsProfile = {
    if (tracers.isEmpty) return null
    var combined: ConcurrentTransactionsTracer.RawStats = null
    tracers.values().forEach { tracer =>
      val raw = tracer.raw
      combined = if (combined == null) raw else combined.merge(raw)
    }
    combined.toProfile
  }
}
