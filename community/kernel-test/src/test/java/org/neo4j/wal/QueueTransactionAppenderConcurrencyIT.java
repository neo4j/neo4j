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
package org.neo4j.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.monitoring.HealthEventGenerator.NO_OP;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.util.concurrent.Futures;
import org.neo4j.wal.files.LogFilesBuilder;

@Neo4jLayoutExtension
@ExtendWith(LifeExtension.class)
@RandomSupportExtension
class QueueTransactionAppenderConcurrencyIT {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private LifeSupport life;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private RandomSupport random;

    private ThreadPoolJobScheduler jobScheduler;
    private TransactionMetadataCache metadataCache;
    private DatabaseHealth databaseHealth;
    private NullLogProvider logProvider;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        jobScheduler = new ThreadPoolJobScheduler();
        logProvider = NullLogProvider.getInstance();
        metadataCache = new TransactionMetadataCache();
        databaseHealth = new DatabaseHealth(NO_OP, logProvider.getLog(DatabaseHealth.class));
        executor = Executors.newFixedThreadPool(20);
    }

    @AfterEach
    void tearDown() {
        executor.shutdown();
        life.shutdown();
        jobScheduler.close();
    }

    @Test
    void multiThreadedTransactionProcessing() throws IOException, ExecutionException {
        WALService walService = buildLogFiles();
        LogMetadataProvider logMetadataProvider = walService.logMetadataProvider();
        life.add(walService);

        QueueTransactionAppender transactionAppender = createAppender(walService);
        life.add(transactionAppender);

        int numberOfTransactions = 10_000;
        long initialCommittedTxId = logMetadataProvider.getLastCommittedTransactionId();

        var results = new ArrayList<Future<?>>(numberOfTransactions);
        for (int i = 0; i < numberOfTransactions; i++) {
            results.add(executor.submit(
                    () -> transactionAppender.register(createTransaction(logMetadataProvider), LogAppendEvent.NULL)));
        }
        Futures.getAll(results);

        assertEquals(logMetadataProvider.getLastCommittedTransactionId(), initialCommittedTxId + numberOfTransactions);
    }

    @Test
    void multiThreadedTransactionWithStop() throws IOException {
        WALService walService = buildLogFiles();
        LogMetadataProvider logMetadataProvider = walService.logMetadataProvider();
        life.add(walService);

        QueueTransactionAppender transactionAppender = createAppender(walService);
        life.add(transactionAppender);

        int numberOfTransactions = 100_000;
        long initialCommittedTxId = logMetadataProvider.getLastCommittedTransactionId();
        var results = new ArrayList<Future<?>>(numberOfTransactions);

        int poisonIndex = random.nextInt(numberOfTransactions / 10, numberOfTransactions - 400);
        for (int i = 0; i < numberOfTransactions; i++) {
            results.add(executor.submit(
                    () -> transactionAppender.register(createTransaction(logMetadataProvider), LogAppendEvent.NULL)));
            if (i == poisonIndex) {
                executor.submit(() -> life.shutdown());
            }
        }

        var futureStatistic = processFutures(results);

        assertEquals(
                logMetadataProvider.getLastCommittedTransactionId(),
                initialCommittedTxId + futureStatistic.getCompletedFutures());
        assertThat(futureStatistic.getFailedFutures()).isNotZero();
        assertThat(futureStatistic.getFailedFutures() + futureStatistic.getCompletedFutures())
                .isEqualTo(numberOfTransactions);
    }

    @Test
    void multiThreadedTransactionWithPanic() throws IOException {
        WALService walService = buildLogFiles();
        LogMetadataProvider logMetadataProvider = walService.logMetadataProvider();
        life.add(walService);

        QueueTransactionAppender transactionAppender = createAppender(walService);
        life.add(transactionAppender);

        int numberOfTransactions = 100_000;
        long initialCommittedTxId = logMetadataProvider.getLastCommittedTransactionId();
        var results = new ArrayList<Future<?>>(numberOfTransactions);

        int poisonIndex = random.nextInt(numberOfTransactions / 10, numberOfTransactions - 400);
        for (int i = 0; i < numberOfTransactions; i++) {
            results.add(executor.submit(
                    () -> transactionAppender.register(createTransaction(logMetadataProvider), LogAppendEvent.NULL)));
            if (i == poisonIndex) {
                executor.submit(
                        () -> databaseHealth.panic(new RuntimeException("Period of intense transaction failures")));
            }
        }

        var futureStatistic = processFutures(results);

        assertEquals(
                logMetadataProvider.getLastCommittedTransactionId(),
                initialCommittedTxId + futureStatistic.getCompletedFutures());
        assertThat(futureStatistic.getFailedFutures()).isNotZero();
        assertThat(futureStatistic.getFailedFutures() + futureStatistic.getCompletedFutures())
                .isEqualTo(numberOfTransactions);
    }

    private QueueTransactionAppender createAppender(WALService walService) {
        LogMetadataProvider logMetadataProvider = walService.logMetadataProvider();
        TransactionLogQueue logQueue = new TransactionLogQueue(
                walService,
                logMetadataProvider,
                databaseHealth,
                logMetadataProvider,
                metadataCache,
                jobScheduler,
                logProvider,
                "le db");
        return new QueueTransactionAppender(logQueue);
    }

    private CompleteTransaction createTransaction(TransactionIdStore transactionIdStore) {
        CompleteCommandBatch tx = new CompleteCommandBatch(
                List.of(new TestCommand()),
                UNKNOWN_CONSENSUS_INDEX,
                1,
                2,
                3,
                4,
                Leases.NO_LEASES,
                LatestVersions.LATEST_KERNEL_VERSION,
                ANONYMOUS);
        var transactionCommitment = new TransactionCommitment(transactionIdStore);
        return new CompleteTransaction(
                tx,
                CursorContext.NULL_CONTEXT,
                StoreCursors.NULL,
                transactionCommitment,
                new IdStoreTransactionIdGenerator(transactionIdStore));
    }

    private WALService buildLogFiles() throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        return LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withRotationThreshold(ByteUnit.mebiBytes(1))
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .build();
    }

    private static FutureStatistic processFutures(List<Future<?>> futures) {
        FutureStatistic statistic = new FutureStatistic();
        for (Future<?> future : futures) {
            try {
                future.get();
                statistic.incrementCompleted();
            } catch (Exception ignored) {
                statistic.incrementFailed();
            }
        }
        return statistic;
    }

    private static final class FutureStatistic {
        int completedFutures;
        int failedFutures;

        void incrementCompleted() {
            completedFutures++;
        }

        void incrementFailed() {
            failedFutures++;
        }

        public int getCompletedFutures() {
            return completedFutures;
        }

        public int getFailedFutures() {
            return failedFutures;
        }
    }
}
