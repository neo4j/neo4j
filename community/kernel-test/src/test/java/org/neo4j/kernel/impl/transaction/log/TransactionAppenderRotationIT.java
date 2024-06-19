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

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.AppendTransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.monitoring.Panic;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

@Neo4jLayoutExtension
@ExtendWith(LifeExtension.class)
class TransactionAppenderRotationIT {
    @Inject
    private DatabaseLayout layout;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private LifeSupport life;

    private final SimpleLogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
    private final SimpleTransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
    private final AppendIndexProvider appendIndexProvider = new SimpleAppendIndexProvider();
    private ThreadPoolJobScheduler jobScheduler;

    @BeforeEach
    void setUp() {
        jobScheduler = new ThreadPoolJobScheduler();
    }

    @AfterEach
    void tearDown() {
        life.shutdown();
        jobScheduler.close();
    }

    @Test
    void correctLastAppliedToPreviousLogTransactionInHeaderOnLogFileRotation()
            throws IOException, ExecutionException, InterruptedException {
        LogFiles logFiles = getLogFiles(logVersionRepository, transactionIdStore, appendIndexProvider);
        life.add(logFiles);
        Panic databasePanic = getDatabaseHealth();

        TransactionAppender transactionAppender = createTransactionAppender(
                logFiles, databasePanic, transactionIdStore, jobScheduler, appendIndexProvider);

        life.add(transactionAppender);

        LogAppendEvent logAppendEvent =
                new RotationLogAppendEvent(logFiles.getLogFile().getLogRotation());
        TransactionToApply transactionToApply = prepareTransaction();
        transactionAppender.append(transactionToApply, logAppendEvent);

        LogFile logFile = logFiles.getLogFile();
        assertEquals(1, logFile.getHighestLogVersion());
        Path highestLogFile = logFile.getHighestLogFile();
        LogHeader logHeader = LogHeaderReader.readLogHeader(fileSystem, highestLogFile, INSTANCE);
        assertEquals(2, logHeader.getLastAppendIndex());
    }

    private static TransactionAppender createTransactionAppender(
            LogFiles logFiles,
            Panic databasePanic,
            TransactionIdStore transactionIdStore,
            JobScheduler scheduler,
            AppendIndexProvider appendIndexProvider) {
        return TransactionAppenderFactory.createTransactionAppender(
                logFiles,
                transactionIdStore,
                appendIndexProvider,
                Config.defaults(),
                databasePanic,
                scheduler,
                NullLogProvider.getInstance(),
                new TransactionMetadataCache());
    }

    private TransactionToApply prepareTransaction() {
        List<StorageCommand> commands = createCommands();
        CompleteTransaction transactionRepresentation = new CompleteTransaction(
                commands, UNKNOWN_CONSENSUS_INDEX, 0, 0, 0, 0, LatestVersions.LATEST_KERNEL_VERSION, ANONYMOUS);
        var transactionCommitment = new TransactionCommitment(transactionIdStore);
        return new TransactionToApply(
                transactionRepresentation,
                NULL_CONTEXT,
                StoreCursors.NULL,
                transactionCommitment,
                new IdStoreTransactionIdGenerator(transactionIdStore));
    }

    private static List<StorageCommand> createCommands() {
        return singletonList(new TestCommand());
    }

    private LogFiles getLogFiles(
            SimpleLogVersionRepository logVersionRepository,
            SimpleTransactionIdStore transactionIdStore,
            AppendIndexProvider appendIndexProvider)
            throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        return LogFilesBuilder.builder(layout, fileSystem, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .withRotationThreshold(ByteUnit.mebiBytes(1))
                .withLogVersionRepository(logVersionRepository)
                .withTransactionIdStore(transactionIdStore)
                .withAppendIndexProvider(appendIndexProvider)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .build();
    }

    private static Panic getDatabaseHealth() {
        return new DatabaseHealth(HealthEventGenerator.NO_OP, NullLog.getInstance());
    }

    private record RotationLogAppendEvent(LogRotation logRotation) implements LogAppendEvent {

        @Override
        public LogForceWaitEvent beginLogForceWait() {
            return null;
        }

        @Override
        public LogForceEvent beginLogForce() {
            return null;
        }

        @Override
        public void appendedBytes(long bytes) {}

        @Override
        public void close() {}

        @Override
        public void setLogRotated(boolean logRotated) {}

        @Override
        public LogRotateEvent beginLogRotate() {
            return null;
        }

        @Override
        public AppendTransactionEvent beginAppendTransaction(int appendItems) {
            return () -> {
                try {
                    logRotation.rotateLogFile(LogAppendEvent.NULL);
                } catch (IOException e) {
                    throw new RuntimeException("Should be able to rotate file", e);
                }
            };
        }
    }
}
