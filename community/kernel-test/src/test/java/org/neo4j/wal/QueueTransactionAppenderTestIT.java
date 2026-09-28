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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.graphdb.DatabaseShutdownException;
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
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.wal.files.LogFilesBuilder;
import org.neo4j.wal.rotation.LogRotateEvent;

@Neo4jLayoutExtension
@ExtendWith(LifeExtension.class)
class QueueTransactionAppenderTestIT {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private LifeSupport life;

    @Inject
    private DatabaseLayout databaseLayout;

    private ThreadPoolJobScheduler jobScheduler;
    private TransactionMetadataCache metadataCache;
    private DatabaseHealth databaseHealth;
    private NullLogProvider logProvider;

    @BeforeEach
    void setUp() {
        jobScheduler = new ThreadPoolJobScheduler();
        logProvider = NullLogProvider.getInstance();
        metadataCache = new TransactionMetadataCache();
        databaseHealth = new DatabaseHealth(HealthEventGenerator.NO_OP, logProvider.getLog(DatabaseHealth.class));
    }

    @AfterEach
    void tearDown() {
        life.shutdown();
        jobScheduler.close();
    }

    @Test
    void sequentialProcessingOfTransaction() throws IOException, ExecutionException, InterruptedException {
        WALService walService = buildLogFiles();
        LogMetadataProvider logMetadataProvider = walService.logMetadataProvider();
        life.add(walService);

        QueueTransactionAppender transactionAppender = createAppender(walService);
        life.add(transactionAppender);

        long txId = logMetadataProvider.getLastCommittedTransactionId();
        for (int i = 0; i < 10; i++) {
            CompleteTransaction completeTransaction = createTransaction(logMetadataProvider);
            assertEquals(++txId, transactionAppender.register(completeTransaction, LogAppendEvent.NULL));
        }
    }

    @Test
    void failToProcessTransactionOnShutdownAppender() throws IOException {
        WALService walService = buildLogFiles();
        LogMetadataProvider logMetadataProvider = walService.logMetadataProvider();
        life.add(walService);

        QueueTransactionAppender transactionAppender = createAppender(walService);
        life.add(transactionAppender);

        CompleteTransaction completeTransaction = createTransaction(logMetadataProvider);
        assertDoesNotThrow(() -> transactionAppender.register(completeTransaction, LogAppendEvent.NULL));

        life.shutdown();

        assertThatThrownBy(() -> transactionAppender.register(completeTransaction, LogAppendEvent.NULL))
                .isInstanceOf(DatabaseShutdownException.class);
    }

    @Test
    void failToProcessTransactionOnNotStartedAppender() throws IOException {
        WALService walService = buildLogFiles();
        LogMetadataProvider logMetadataProvider = walService.logMetadataProvider();
        life.add(walService);

        QueueTransactionAppender transactionAppender = createAppender(walService);

        CompleteTransaction completeTransaction = createTransaction(logMetadataProvider);
        assertThatThrownBy(() -> transactionAppender.register(completeTransaction, LogAppendEvent.NULL))
                .isInstanceOf(DatabaseShutdownException.class);
    }

    @Test
    void registerTransactionAsCommittedOnProcessing() throws IOException, ExecutionException, InterruptedException {
        WALService walService = buildLogFiles();
        LogMetadataProvider logMetadataProvider = walService.logMetadataProvider();
        life.add(walService);

        QueueTransactionAppender transactionAppender = createAppender(walService);
        life.add(transactionAppender);

        long initialLastCommittedTxId = logMetadataProvider.getLastCommittedTransactionId();
        long initialLastClosedTxId = logMetadataProvider.getHighestGapFreeClosedTransactionId();
        int numberOfTransactions = 10;
        for (int i = 0; i < numberOfTransactions; i++) {
            CompleteTransaction completeTransaction = createTransaction(logMetadataProvider);
            transactionAppender.register(completeTransaction, LogAppendEvent.NULL);
        }

        assertEquals(
                initialLastCommittedTxId + numberOfTransactions, logMetadataProvider.getLastCommittedTransactionId());
        assertEquals(initialLastClosedTxId, logMetadataProvider.getHighestGapFreeClosedTransactionId());
    }

    @Test
    void failToProcessTransactionOnNonHealthyDatabase() throws IOException {
        WALService walService = buildLogFiles();
        LogMetadataProvider logMetadataProvider = walService.logMetadataProvider();
        life.add(walService);

        QueueTransactionAppender transactionAppender = createAppender(walService);
        life.add(transactionAppender);

        RuntimeException panicException = new RuntimeException("Don't panic, the answer is known!");
        databaseHealth.panic(panicException);

        CompleteTransaction completeTransaction = createTransaction(logMetadataProvider);
        assertThatThrownBy(() -> transactionAppender.register(completeTransaction, LogAppendEvent.NULL))
                .hasRootCause(panicException);
    }

    @Test
    void processTransactionWithProperEvents() throws IOException, ExecutionException, InterruptedException {
        WALService walService = buildLogFiles();
        LogMetadataProvider logMetadataProvider = walService.logMetadataProvider();
        life.add(walService);

        QueueTransactionAppender transactionAppender = createAppender(walService);
        life.add(transactionAppender);

        CompleteTransaction completeTransaction = createTransaction(logMetadataProvider);
        RecordingLogAppendEvent logAppendEvent = new RecordingLogAppendEvent();
        transactionAppender.register(completeTransaction, logAppendEvent);
        assertThat(logAppendEvent.getEvents())
                .containsExactly(
                        EventType.BEGIN_APPEND,
                        EventType.FILE_APPEND,
                        EventType.CLOSE_APPEND,
                        EventType.ROTATED_FALSE,
                        EventType.LOG_FORCE);
    }

    @Test
    void failureOnProcessingUpdatesDatabaseHealth() throws IOException {
        WALService walService = buildLogFiles();
        LogMetadataProvider logMetadataProvider = walService.logMetadataProvider();
        life.add(walService);

        QueueTransactionAppender transactionAppender = createAppender(walService);
        life.add(transactionAppender);

        RuntimeException criticalException = new RuntimeException("The greatest teacher, failure is.");
        CompleteTransaction completeTransaction = createTransaction(logMetadataProvider);
        assertThatThrownBy(() -> transactionAppender.register(completeTransaction, new LogAppendEvent.Empty() {
                    @Override
                    public LogForceEvent beginLogForce() {
                        throw criticalException;
                    }
                }))
                .rootCause()
                .hasMessageContaining("failure is.");

        assertFalse(databaseHealth.hasNoPanic());
        assertThat(databaseHealth.causeOfPanic()).isSameAs(criticalException);

        assertThatThrownBy(() -> transactionAppender.register(completeTransaction, LogAppendEvent.NULL))
                .rootCause()
                .hasMessageContaining("failure is.");
    }

    private QueueTransactionAppender createAppender(WALService walService) {
        var logQueue = new TransactionLogQueue(
                walService,
                walService.logMetadataProvider(),
                databaseHealth,
                walService.logMetadataProvider(),
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

    private enum EventType {
        FILE_APPEND,
        ROTATE,
        ROTATED_TRUE,
        ROTATED_FALSE,
        BEGIN_APPEND,
        CLOSE_APPEND,
        LOG_FORCE_WAIT,
        LOG_FORCE,
        CLOSE
    }

    private static class RecordingLogAppendEvent implements LogAppendEvent {
        private final Queue<EventType> events = new LinkedBlockingQueue<>();

        @Override
        public void appendedBytes(long bytes) {
            events.add(EventType.FILE_APPEND);
        }

        @Override
        public void close() {
            events.add(EventType.CLOSE);
        }

        @Override
        public void setLogRotated(boolean logRotated) {
            var event = logRotated ? EventType.ROTATED_TRUE : EventType.ROTATED_FALSE;
            events.add(event);
        }

        @Override
        public AppendTransactionEvent beginAppendTransaction(int appendItems) {
            events.add(EventType.BEGIN_APPEND);
            return new RecordingTransactionAppendEvent(events);
        }

        @Override
        public LogForceWaitEvent beginLogForceWait() {
            events.add(EventType.LOG_FORCE_WAIT);
            return LogForceWaitEvent.NULL;
        }

        @Override
        public LogForceEvent beginLogForce() {
            events.add(EventType.LOG_FORCE);
            return LogForceEvent.NULL;
        }

        @Override
        public LogRotateEvent beginLogRotate() {
            events.add(EventType.ROTATE);
            return LogRotateEvent.NULL;
        }

        public Queue<EventType> getEvents() {
            return events;
        }
    }

    private static class RecordingTransactionAppendEvent implements AppendTransactionEvent {
        private final Queue<EventType> events;

        RecordingTransactionAppendEvent(Queue<EventType> events) {
            this.events = events;
        }

        @Override
        public void close() {
            events.add(EventType.CLOSE_APPEND);
        }
    }
}
