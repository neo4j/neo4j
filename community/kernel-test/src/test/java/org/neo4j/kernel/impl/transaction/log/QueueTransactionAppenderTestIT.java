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
package org.neo4j.kernel.impl.transaction.log;

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
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.tracing.AppendTransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

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
    private SimpleLogVersionRepository logVersionRepository;
    private SimpleTransactionIdStore transactionIdStore;
    private TransactionMetadataCache metadataCache;
    private DatabaseHealth databaseHealth;
    private NullLogProvider logProvider;
    private SimpleAppendIndexProvider appendIndexProvider;

    @BeforeEach
    void setUp() {
        jobScheduler = new ThreadPoolJobScheduler();

        logVersionRepository = new SimpleLogVersionRepository();
        transactionIdStore = new SimpleTransactionIdStore();
        appendIndexProvider = new SimpleAppendIndexProvider();
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
        LogFiles logFiles = buildLogFiles(logVersionRepository, transactionIdStore, appendIndexProvider);
        life.add(logFiles);

        QueueTransactionAppender transactionAppender = createAppender(logFiles);
        life.add(transactionAppender);

        long txId = transactionIdStore.getLastCommittedTransactionId();
        for (int i = 0; i < 10; i++) {
            TransactionToApply transactionToApply = createTransaction();
            assertEquals(++txId, transactionAppender.append(transactionToApply, LogAppendEvent.NULL));
        }
    }

    @Test
    void failToProcessTransactionOnShutdownAppender() throws IOException {
        LogFiles logFiles = buildLogFiles(logVersionRepository, transactionIdStore, appendIndexProvider);
        life.add(logFiles);

        QueueTransactionAppender transactionAppender = createAppender(logFiles);
        life.add(transactionAppender);

        TransactionToApply transactionToApply = createTransaction();
        assertDoesNotThrow(() -> transactionAppender.append(transactionToApply, LogAppendEvent.NULL));

        life.shutdown();

        assertThatThrownBy(() -> transactionAppender.append(transactionToApply, LogAppendEvent.NULL))
                .isInstanceOf(DatabaseShutdownException.class);
    }

    @Test
    void failToProcessTransactionOnNotStartedAppender() throws IOException {
        LogFiles logFiles = buildLogFiles(logVersionRepository, transactionIdStore, appendIndexProvider);
        life.add(logFiles);

        QueueTransactionAppender transactionAppender = createAppender(logFiles);

        TransactionToApply transactionToApply = createTransaction();
        assertThatThrownBy(() -> transactionAppender.append(transactionToApply, LogAppendEvent.NULL))
                .isInstanceOf(DatabaseShutdownException.class);
    }

    @Test
    void publishTransactionAsCommittedOnProcessing() throws IOException, ExecutionException, InterruptedException {
        LogFiles logFiles = buildLogFiles(logVersionRepository, transactionIdStore, appendIndexProvider);
        life.add(logFiles);

        QueueTransactionAppender transactionAppender = createAppender(logFiles);
        life.add(transactionAppender);

        long initialLastCommittedTxId = transactionIdStore.getLastCommittedTransactionId();
        long initialLastClosedTxId = transactionIdStore.getLastClosedTransactionId();
        int numberOfTransactions = 10;
        for (int i = 0; i < numberOfTransactions; i++) {
            TransactionToApply transactionToApply = createTransaction();
            transactionAppender.append(transactionToApply, LogAppendEvent.NULL);
        }

        assertEquals(
                initialLastCommittedTxId + numberOfTransactions, transactionIdStore.getLastCommittedTransactionId());
        assertEquals(initialLastClosedTxId, transactionIdStore.getLastClosedTransactionId());
    }

    @Test
    void failToProcessTransactionOnNonHealthyDatabase() throws IOException {
        LogFiles logFiles = buildLogFiles(logVersionRepository, transactionIdStore, appendIndexProvider);
        life.add(logFiles);

        QueueTransactionAppender transactionAppender = createAppender(logFiles);
        life.add(transactionAppender);

        RuntimeException panicException = new RuntimeException("Don't panic, the answer is known!");
        databaseHealth.panic(panicException);

        TransactionToApply transactionToApply = createTransaction();
        assertThatThrownBy(() -> transactionAppender.append(transactionToApply, LogAppendEvent.NULL))
                .hasRootCause(panicException);
    }

    @Test
    void processTransactionWithProperEvents() throws IOException, ExecutionException, InterruptedException {
        LogFiles logFiles = buildLogFiles(logVersionRepository, transactionIdStore, appendIndexProvider);
        life.add(logFiles);

        QueueTransactionAppender transactionAppender = createAppender(logFiles);
        life.add(transactionAppender);

        TransactionToApply transactionToApply = createTransaction();
        RecordingLogAppendEvent logAppendEvent = new RecordingLogAppendEvent();
        transactionAppender.append(transactionToApply, logAppendEvent);
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
        LogFiles logFiles = buildLogFiles(logVersionRepository, transactionIdStore, appendIndexProvider);
        life.add(logFiles);

        QueueTransactionAppender transactionAppender = createAppender(logFiles);
        life.add(transactionAppender);

        RuntimeException criticalException = new RuntimeException("The greatest teacher, failure is.");
        TransactionToApply transactionToApply = createTransaction();
        assertThatThrownBy(() -> transactionAppender.append(transactionToApply, new LogAppendEvent.Empty() {
                    @Override
                    public LogForceEvent beginLogForce() {
                        throw criticalException;
                    }
                }))
                .rootCause()
                .hasMessageContaining("failure is.");

        assertFalse(databaseHealth.hasNoPanic());
        assertThat(databaseHealth.causeOfPanic()).isSameAs(criticalException);

        assertThatThrownBy(() -> transactionAppender.append(transactionToApply, LogAppendEvent.NULL))
                .rootCause()
                .hasMessageContaining("failure is.");
    }

    private QueueTransactionAppender createAppender(LogFiles logFiles) {
        var logQueue = new TransactionLogQueue(
                logFiles,
                transactionIdStore,
                databaseHealth,
                appendIndexProvider,
                metadataCache,
                jobScheduler,
                logProvider);
        return new QueueTransactionAppender(logQueue);
    }

    private TransactionToApply createTransaction() {
        CompleteTransaction tx = new CompleteTransaction(
                List.of(new TestCommand()),
                UNKNOWN_CONSENSUS_INDEX,
                1,
                2,
                3,
                4,
                LatestVersions.LATEST_KERNEL_VERSION,
                ANONYMOUS);
        var transactionCommitment = new TransactionCommitment(transactionIdStore);
        return new TransactionToApply(
                tx,
                CursorContext.NULL_CONTEXT,
                StoreCursors.NULL,
                transactionCommitment,
                new IdStoreTransactionIdGenerator(transactionIdStore));
    }

    private LogFiles buildLogFiles(
            SimpleLogVersionRepository logVersionRepository,
            SimpleTransactionIdStore transactionIdStore,
            AppendIndexProvider appendIndexProvider)
            throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        return LogFilesBuilder.builder(databaseLayout, fileSystem, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .withLogVersionRepository(logVersionRepository)
                .withRotationThreshold(ByteUnit.mebiBytes(1))
                .withTransactionIdStore(transactionIdStore)
                .withAppendIndexProvider(appendIndexProvider)
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
