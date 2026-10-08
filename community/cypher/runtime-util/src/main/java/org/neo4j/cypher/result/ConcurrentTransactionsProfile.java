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
package org.neo4j.cypher.result;

/**
 * Profile of the runtime concurrency achieved by CONCURRENT TRANSACTIONS operators
 * (TransactionApply / TransactionForeach). Recorded by the {@code ConcurrentTransactionsTracer}
 * whenever a query starts concurrent transactions; populated per operator (PROFILE plan
 * description) and aggregated per query (query statistics).
 *
 * <p>All metrics are defined on the batch <em>attempt</em> lifecycle, independent of how a runtime
 * schedules the work:
 * <ul>
 *   <li><b>queued</b>: the attempt is fully formed (accepts no more rows) and awaits execution,
 *       whether held back by conflict-avoiding scheduling, staged, or waiting for a free worker. A
 *       first attempt becomes queued when batch formation completes; a retry attempt (ON ERROR
 *       RETRY) becomes queued when its backoff delay has elapsed and it is polled for re-execution,
 *       so deliberate backoff waiting is not counted.</li>
 *   <li><b>in-flight</b>: a worker is processing the attempt (from worker start to worker finish).</li>
 * </ul>
 *
 * <ul>
 *   <li>{@code peakConcurrency} / {@code avgConcurrency}: max and time-weighted average number of
 *       attempts in flight. The average spans first worker start to last worker end.</li>
 *   <li>{@code peakQueuedBatches} / {@code avgQueuedBatches}: max and time-weighted average number
 *       of queued attempts. The average spans the first queued event to the last lifecycle event.
 *       Together with concurrency this separates the bottlenecks: low concurrency with an empty
 *       queue means the operator was starved for input, low concurrency with a deep queue means
 *       runnable work could not be dispatched (for example conflict-avoiding scheduling), and full
 *       concurrency with a deep queue means the workers were saturated.</li>
 *   <li>{@code peakStartGapNanos} / {@code avgStartGapNanos}: per-attempt latency from queued to
 *       worker start.</li>
 *   <li>{@code concurrencyLimit}: the effective maximum concurrency the operator ran with (the
 *       resolved {@code IN [n] CONCURRENT TRANSACTIONS} value).</li>
 *   <li>{@code batchesQueued} / {@code batchesStarted}: counted per attempt (a retried batch is
 *       counted again).</li>
 *   <li>{@code innerTransactionsStarted} / {@code innerTransactionsCommitted}: counted per attempt;
 *       committed only counts successful commits, so started &gt;= committed by design.</li>
 *   <li>{@code retries}: number of times a batch was scheduled for retry (ON ERROR RETRY).</li>
 * </ul>
 *
 * <p>Caveats. A snapshot taken while the query is still executing is a point-in-time approximation:
 * the worker time of attempts still in flight is not yet accumulated, so averages under-report
 * until the operator completes. In the query-level aggregate over multiple CONCURRENT TRANSACTIONS
 * operators, averages are time-weighted over the whole envelope from the earliest start to the
 * latest lifecycle event across operators, not the union of their active spans: an idle gap between
 * non-overlapping operators dilutes the average, and overlapping operators can push the average
 * concurrency above {@code concurrencyLimit}. Peaks and {@code concurrencyLimit} are the maximum
 * within any single operator, so per-operator PROFILE values can legitimately differ from the
 * query-level statistics.
 */
public record ConcurrentTransactionsProfile(
        long peakConcurrency,
        double avgConcurrency,
        long peakQueuedBatches,
        double avgQueuedBatches,
        long peakStartGapNanos,
        double avgStartGapNanos,
        long concurrencyLimit,
        long batchesQueued,
        long batchesStarted,
        long innerTransactionsStarted,
        long innerTransactionsCommitted,
        long retries) {}
