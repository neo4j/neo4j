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
package org.neo4j.kernel.api.query;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.internal.kernel.api.ExecutionStatistics;
import org.neo4j.util.VisibleForTesting;

/**
 * Aggregated statistics of transactions that have executed a query but are already committed.
 */
interface QueryTransactionStatisticsAggregator {
    /**
     * Record the page cache statistics of a transaction that is about to close.
     */
    void recordStatisticsOfTransactionAboutToClose(long hits, long faults, long transactionSequenceNumber);

    /**
     * Record the page cache statistics of a transaction that has closed.
     */
    void recordStatisticsOfClosedTransaction(long hits, long faults, long transactionSequenceNumber);

    /**
     * Get the total page hits of closed transactions.
     */
    long pageHitsOfClosedTransactions();

    /**
     * Get the total page faults of closed transactions.
     */
    long pageFaultsOfClosedTransactions();

    /**
     * Get the page hits of closed transaction commits.
     */
    long pageHitsOfClosedTransactionCommits();

    /**
     * Get the page faults of closed transaction commits.
     */
    long pageFaultsOfClosedTransactionCommits();

    /**
     * Get a consistent execution statistics snapshot of closed transactions excluding commits.
     */
    @VisibleForTesting
    ExecutionStatistics statisticsOfClosedTransactionsExcludingCommits();

    /**
     * Get a consistent execution statistics snapshot of closed transaction commits.
     */
    @VisibleForTesting
    ExecutionStatistics statisticsOfClosedTransactionCommits();

    /**
     * An of {@link QueryTransactionStatisticsAggregator} that uses volatile fields for read visibility
     * but only supports a single writer thread to record statistics.
     */
    class DefaultImpl implements QueryTransactionStatisticsAggregator {
        private volatile long pageHitsOfClosedTransactionsExcludingCommits;
        private volatile long pageFaultsOfClosedTransactionsExcludingCommits;
        private volatile long pageHitsOfClosedTransactionsIncludingCommits;
        private volatile long pageFaultsOfClosedTransactionsIncludingCommits;
        private volatile long pageHitsOfClosedTransactionCommits;
        private volatile long pageFaultsOfClosedTransactionCommits;

        @Override
        public void recordStatisticsOfTransactionAboutToClose(long hits, long faults, long transactionSequenceNumber) {
            // We only have one thread writing to these fields. Ignore transactionSequenceNumber.
            //noinspection NonAtomicOperationOnVolatileField
            pageHitsOfClosedTransactionsExcludingCommits += hits;
            //noinspection NonAtomicOperationOnVolatileField
            pageFaultsOfClosedTransactionsExcludingCommits += faults;
        }

        /**
         * A transaction executing part of this query is closing; record its page cache statistics (including commit).
         */
        @Override
        public void recordStatisticsOfClosedTransaction(long hits, long faults, long transactionSequenceNumber) {
            // We only have one thread writing to these fields
            //noinspection NonAtomicOperationOnVolatileField
            pageHitsOfClosedTransactionsIncludingCommits += hits;
            //noinspection NonAtomicOperationOnVolatileField
            pageFaultsOfClosedTransactionsIncludingCommits += faults;

            pageHitsOfClosedTransactionCommits =
                    (pageHitsOfClosedTransactionsIncludingCommits - pageHitsOfClosedTransactionsExcludingCommits);
            pageFaultsOfClosedTransactionCommits =
                    (pageFaultsOfClosedTransactionsIncludingCommits - pageFaultsOfClosedTransactionsExcludingCommits);
        }

        public long pageHitsOfClosedTransactions() {
            return pageHitsOfClosedTransactionsExcludingCommits + pageHitsOfClosedTransactionCommits;
        }

        public long pageFaultsOfClosedTransactions() {
            return pageFaultsOfClosedTransactionsExcludingCommits + pageFaultsOfClosedTransactionCommits;
        }

        public long pageHitsOfClosedTransactionCommits() {
            return pageHitsOfClosedTransactionCommits;
        }

        public long pageFaultsOfClosedTransactionCommits() {
            return pageFaultsOfClosedTransactionCommits;
        }

        @Override
        public ExecutionStatistics statisticsOfClosedTransactionsExcludingCommits() {
            return new Stats(
                    pageHitsOfClosedTransactionsExcludingCommits, pageFaultsOfClosedTransactionsExcludingCommits, 0);
        }

        @Override
        public ExecutionStatistics statisticsOfClosedTransactionCommits() {
            return new Stats(pageHitsOfClosedTransactionCommits, pageFaultsOfClosedTransactionCommits, 0);
        }
    }

    /**
     * An implementation of {@link QueryTransactionStatisticsAggregator}
     * that supports concurrent writer threads to record statistics.
     * <p>
     * IMPORTANT: Only one thread at a time is allowed to record statistics for a given transaction sequence number.
     * <p>
     * NOTE:
     */
    class ConcurrentImpl implements QueryTransactionStatisticsAggregator {

        private final Map<Long, Stats> pageStatsExcludingCommits = new ConcurrentHashMap<>();
        // private Map<Long, Stats> pageStatsIncludingCommits = new ConcurrentHashMap<>();

        private final AtomicReference<Stats> statsOfClosedTransactionsExcludingCommits =
                new AtomicReference<>(new Stats(0, 0, 0));

        private final AtomicReference<Stats> statsOfClosedTransactionCommits =
                new AtomicReference<>(new Stats(0, 0, 0));

        public ConcurrentImpl() {}

        public ConcurrentImpl(QueryTransactionStatisticsAggregator statistics) {
            var hitsIncluding = statistics.pageHitsOfClosedTransactions();
            var faultsIncluding = statistics.pageFaultsOfClosedTransactions();
            var hitsOfCommits = statistics.pageHitsOfClosedTransactionCommits();
            var faultsOfCommits = statistics.pageFaultsOfClosedTransactionCommits();
            var hitsExcluding = hitsIncluding - hitsOfCommits;
            var faultsExcluding = faultsIncluding - faultsOfCommits;
            var statsExcludingCommits = statsOfClosedTransactionsExcludingCommits.get();
            var statsOfCommits = statsOfClosedTransactionCommits.get();
            statsExcludingCommits.hits = hitsExcluding;
            statsExcludingCommits.faults = faultsExcluding;
            statsOfCommits.hits = hitsOfCommits;
            statsOfCommits.faults = faultsOfCommits;
        }

        @Override
        public void recordStatisticsOfTransactionAboutToClose(long hits, long faults, long transactionSequenceNumber) {
            // NOTE: We assume that only one thread is recording statistics for a given transaction sequence number.
            var stats = pageStatsExcludingCommits.get(transactionSequenceNumber);
            if (stats == null) {
                pageStatsExcludingCommits.put(
                        transactionSequenceNumber, new Stats(hits, faults, transactionSequenceNumber));
            } else {
                stats.hits += hits;
                stats.faults += faults;
            }
            updateStats(statsOfClosedTransactionsExcludingCommits, hits, faults, transactionSequenceNumber);
        }

        @Override
        public void recordStatisticsOfClosedTransaction(long hits, long faults, long transactionSequenceNumber) {
            // NOTE: We assume that only one thread is recording statistics for a given transaction sequence number.
            var excludingCommitsStats = pageStatsExcludingCommits.remove(transactionSequenceNumber);
            if (excludingCommitsStats == null) {
                throw new IllegalStateException(
                        "Expected to find recorded page cache statistics for transaction sequence number "
                                + transactionSequenceNumber + " on thread "
                                + Thread.currentThread().getName());
            }
            var hitsDuringCommit = hits - excludingCommitsStats.hits;
            var faultsDuringCommit = faults - excludingCommitsStats.faults;
            updateStats(
                    statsOfClosedTransactionCommits, hitsDuringCommit, faultsDuringCommit, transactionSequenceNumber);
        }

        private static void updateStats(
                AtomicReference<Stats> stats, long hits, long faults, long transactionSequenceNumber) {
            Stats current;
            Stats updated = new Stats(0, 0, transactionSequenceNumber);
            do {
                current = stats.get();
                updated.hits = current.hits + hits;
                updated.faults = current.faults + faults;
                // Keep the highest observed transaction sequence number
                var currentSequenceNumber = current.transactionSequenceNumber;
                if (currentSequenceNumber > transactionSequenceNumber) {
                    updated.transactionSequenceNumber = currentSequenceNumber;
                }
            } while (!stats.weakCompareAndSetVolatile(current, updated));
        }

        @Override
        public long pageHitsOfClosedTransactions() {
            return statsOfClosedTransactionsExcludingCommits.get().hits + statsOfClosedTransactionCommits.get().hits;
        }

        @Override
        public long pageFaultsOfClosedTransactions() {
            return statsOfClosedTransactionsExcludingCommits.get().faults
                    + statsOfClosedTransactionCommits.get().faults;
        }

        @Override
        public long pageHitsOfClosedTransactionCommits() {
            return statsOfClosedTransactionCommits.get().hits;
        }

        @Override
        public long pageFaultsOfClosedTransactionCommits() {
            return statsOfClosedTransactionCommits.get().faults;
        }

        @Override
        public ExecutionStatistics statisticsOfClosedTransactionsExcludingCommits() {
            return statsOfClosedTransactionsExcludingCommits.get();
        }

        @Override
        public ExecutionStatistics statisticsOfClosedTransactionCommits() {
            return statsOfClosedTransactionCommits.get();
        }
    }

    class Stats implements ExecutionStatistics {
        long hits;
        long faults;
        long transactionSequenceNumber;

        Stats(long hits, long faults, long transactionSequenceNumber) {
            this.hits = hits;
            this.faults = faults;
            this.transactionSequenceNumber = transactionSequenceNumber;
        }

        @Override
        public long pageHits() {
            return hits;
        }

        @Override
        public long pageFaults() {
            return faults;
        }

        @Override
        public long getTransactionSequenceNumber() {
            return transactionSequenceNumber;
        }
    }
}
