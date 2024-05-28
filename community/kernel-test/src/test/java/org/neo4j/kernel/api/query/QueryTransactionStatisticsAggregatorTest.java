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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

class QueryTransactionStatisticsAggregatorTest {
    @Test
    void concurrentStressTest() {
        var timeout = TimeUnit.MINUTES.toMillis(10);
        var numberOfThreads = 10;
        var numberOfTransactions = 10000;
        var aggregator = new QueryTransactionStatisticsAggregator.ConcurrentImpl();
        var commitPhaseHits = new LongAdder();
        var commitPhaseFaults = new LongAdder();
        var queue = new ArrayBlockingQueue<Runnable>(numberOfTransactions);
        var executor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 10, TimeUnit.SECONDS, queue);
        for (int i = 0; i < numberOfTransactions; i++) {
            executor.execute(new Task(aggregator, i, commitPhaseHits, commitPhaseFaults));
        }
        executor.shutdown();

        // While transactions are running we verify that the statistics are consistent.
        var startTime = System.currentTimeMillis();
        var highestTransactionId1 = 0L;
        var highestTransactionId2 = 0L;
        while (!executor.isTerminated()) {
            var stats1 = aggregator.statisticsOfClosedTransactionsExcludingCommits();
            var stats2 = aggregator.statisticsOfClosedTransactionCommits();

            // Page hits and faults should match
            assertThat(stats1.pageHits()).isEqualTo(stats1.pageFaults());
            assertThat(stats2.pageHits()).isEqualTo(stats2.pageFaults());

            // Transaction ids should be increasing
            var transactionId1 = stats1.getTransactionSequenceNumber();
            var transactionId2 = stats2.getTransactionSequenceNumber();
            assertThat(transactionId1).isGreaterThanOrEqualTo(highestTransactionId1);
            assertThat(transactionId2).isGreaterThanOrEqualTo(highestTransactionId2);
            highestTransactionId1 = transactionId1;
            highestTransactionId2 = transactionId2;

            if (System.currentTimeMillis() - startTime > timeout) {
                fail("Test timed out after " + timeout + " ms.");
            }
        }

        // Totals should add up
        assertThat(aggregator.pageHitsOfClosedTransactions()).isEqualTo(numberOfTransactions * 3);
        assertThat(aggregator.pageFaultsOfClosedTransactions()).isEqualTo(numberOfTransactions * 3);
        assertThat(aggregator.pageHitsOfClosedTransactionCommits()).isEqualTo(numberOfTransactions);
        assertThat(aggregator.pageFaultsOfClosedTransactionCommits()).isEqualTo(numberOfTransactions);
        assertThat(commitPhaseHits.sum()).isEqualTo(numberOfTransactions);
        assertThat(commitPhaseFaults.sum()).isEqualTo(numberOfTransactions);
    }

    private record Task(
            QueryTransactionStatisticsAggregator aggregator,
            long transactionId,
            LongAdder commitPhaseHits,
            LongAdder commitPhaseFaults)
            implements Runnable {
        private static final Random RANDOM = new Random();

        @Override
        public void run() {
            aggregator.recordStatisticsOfTransactionAboutToClose(2, 2, transactionId);
            LockSupport.parkNanos(RANDOM.nextLong(1000, 10_000));
            aggregator.recordStatisticsOfClosedTransaction(3, 3, transactionId, (hits, faults, txId) -> {
                commitPhaseHits.add(hits);
                commitPhaseFaults.add(faults);
            });
        }
    }
}
