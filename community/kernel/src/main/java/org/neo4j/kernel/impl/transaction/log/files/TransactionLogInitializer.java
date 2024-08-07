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
package org.neo4j.kernel.impl.transaction.log.files;

import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.kernel.impl.api.CompleteTransaction.NOT_SPECIFIED_CHUNK_ID;
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.database.MetadataCache;
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionId;

/**
 * Provides methods for ensuring that transaction log files are properly initialised for a store.
 * This includes making sure that the log files are ready to be replicated in a cluster.
 */
public class TransactionLogInitializer {
    private final FileSystemAbstraction fs;
    private final MetadataProvider metadataProvider;
    private final StorageEngineFactory storageEngineFactory;
    private final MetadataCache metadataCache;

    /**
     * Get a {@link LogFilesInitializer} implementation, suitable for e.g. passing to a batch importer.
     * @return A {@link LogFilesInitializer} instance.
     */
    public static LogFilesInitializer getLogFilesInitializer() {
        return new LogFilesInitializer() {
            @Override
            public void initializeLogFiles(
                    DatabaseLayout databaseLayout,
                    MetadataProvider metadataProvider,
                    MetadataCache metadataCache,
                    FileSystemAbstraction fileSystem,
                    String checkpointReason) {
                try {
                    TransactionLogInitializer initializer = new TransactionLogInitializer(
                            fileSystem,
                            metadataProvider,
                            StorageEngineFactory.selectStorageEngine(fileSystem, databaseLayout)
                                    .orElseThrow(),
                            metadataCache);
                    initializer.initializeEmptyLogFile(
                            databaseLayout, databaseLayout.getTransactionLogsDirectory(), checkpointReason);
                } catch (IOException e) {
                    throw new UnderlyingStorageException("Failed to initialize transaction log files.", e);
                }
            }

            @Override
            public void clearHistoryAndInitializeLogFiles(
                    DatabaseLayout databaseLayout,
                    MetadataProvider metadataProvider,
                    MetadataCache metadataCache,
                    FileSystemAbstraction fileSystem,
                    String checkpointReason) {
                try {
                    TransactionLogInitializer initializer = new TransactionLogInitializer(
                            fileSystem,
                            metadataProvider,
                            StorageEngineFactory.selectStorageEngine(fileSystem, databaseLayout)
                                    .orElseThrow(),
                            metadataCache);
                    initializer.migrateExistingLogFiles(
                            databaseLayout, databaseLayout.getTransactionLogsDirectory(), checkpointReason);
                } catch (Exception e) {
                    throw new UnderlyingStorageException(
                            "Failed to clear history and initialize transaction log files.", e);
                }
            }
        };
    }

    public TransactionLogInitializer(
            FileSystemAbstraction fs,
            MetadataProvider metadataProvider,
            StorageEngineFactory storageEngineFactory,
            MetadataCache metadataCache) {
        this.fs = fs;
        this.metadataProvider = metadataProvider;
        this.storageEngineFactory = storageEngineFactory;
        this.metadataCache = metadataCache;
    }

    /**
     * Create new empty log files in the given transaction logs directory, for a database that doesn't have any already.
     */
    public long initializeEmptyLogFile(DatabaseLayout layout, Path transactionLogsDirectory, String checkpointReason)
            throws IOException {
        try (LogFilesSpan span = buildLogFiles(layout, transactionLogsDirectory)) {
            LogFiles logFiles = span.getLogFiles();
            return appendEmptyTransactionAndCheckPoint(logFiles, checkpointReason);
        }
    }

    public long migrateExistingLogFiles(DatabaseLayout layout, Path transactionLogsDirectory, String checkpointReason)
            throws Exception {
        try (LogFilesSpan span = buildLogFiles(layout, transactionLogsDirectory)) {
            LogFiles logFiles = span.getLogFiles();
            LogFile logFile = logFiles.getLogFile();
            for (long version = logFile.getLowestLogVersion(); version <= logFile.getHighestLogVersion(); version++) {
                fs.deleteFile(logFile.getLogFileForVersion(version));
            }
            CheckpointFile checkpointFile = logFiles.getCheckpointFile();
            for (long version = checkpointFile.getLowestLogVersion();
                    version <= checkpointFile.getHighestLogVersion();
                    version++) {
                fs.deleteFile(checkpointFile.getDetachedCheckpointFileForVersion(version));
            }
            logFile.rotate();
            checkpointFile.rotate();
            return appendEmptyTransactionAndCheckPoint(logFiles, checkpointReason);
        }
    }

    private LogFilesSpan buildLogFiles(DatabaseLayout layout, Path transactionLogsDirectory) throws IOException {
        LogFiles logFiles = LogFilesBuilder.builder(layout, fs, metadataCache)
                .withLogVersionRepository(metadataProvider)
                .withTransactionIdStore(metadataProvider)
                .withAppendIndexProvider(metadataProvider)
                .withStoreId(metadataProvider.getStoreId())
                .withLogsDirectory(transactionLogsDirectory)
                .withStorageEngineFactory(storageEngineFactory)
                .withDatabaseHealth(new DatabaseHealth(HealthEventGenerator.NO_OP, NullLog.getInstance()))
                .build();
        return new LogFilesSpan(new Lifespan(logFiles), logFiles);
    }

    private long appendEmptyTransactionAndCheckPoint(LogFiles logFiles, String reason) throws IOException {
        TransactionId committedTx = metadataProvider.getLastCommittedTransaction();
        long consensusIndex = UNKNOWN_CONSENSUS_INDEX;
        long timestamp = committedTx.commitTimestamp();
        long upgradeTransactionId = metadataProvider.nextCommittingTransactionId();
        long appendIndex = metadataProvider.nextAppendIndex();
        KernelVersion kernelVersion = metadataCache.kernelVersion();
        LogFile logFile = logFiles.getLogFile();
        TransactionLogWriter transactionLogWriter = logFile.getTransactionLogWriter();
        CompleteCommandBatch emptyTx = emptyTransaction(timestamp, upgradeTransactionId, kernelVersion, consensusIndex);
        int checksum = transactionLogWriter.append(
                emptyTx,
                upgradeTransactionId,
                NOT_SPECIFIED_CHUNK_ID,
                appendIndex,
                BASE_TX_CHECKSUM,
                LogPosition.UNSPECIFIED,
                LogAppendEvent.NULL);
        logFile.forceAfterAppend(LogAppendEvent.NULL);
        LogPosition position = transactionLogWriter.getCurrentPosition();
        appendCheckpoint(
                logFiles,
                reason,
                position,
                new TransactionId(
                        upgradeTransactionId, appendIndex, kernelVersion, checksum, timestamp, consensusIndex),
                appendIndex,
                kernelVersion);
        metadataProvider.transactionCommitted(
                upgradeTransactionId, appendIndex, kernelVersion, checksum, timestamp, consensusIndex);
        return upgradeTransactionId;
    }

    private static CompleteCommandBatch emptyTransaction(
            long timestamp, long txId, KernelVersion kernelVersion, long consensusIndex) {
        return new CompleteCommandBatch(
                Collections.emptyList(),
                consensusIndex,
                timestamp,
                txId,
                timestamp,
                NO_LEASE,
                kernelVersion,
                ANONYMOUS);
    }

    private static void appendCheckpoint(
            LogFiles logFiles,
            String reason,
            LogPosition position,
            TransactionId transactionId,
            long appendIndex,
            KernelVersion version)
            throws IOException {
        var checkpointAppender = logFiles.getCheckpointFile().getCheckpointAppender();
        checkpointAppender.checkPoint(
                LogCheckPointEvent.NULL,
                transactionId,
                appendIndex,
                version,
                position,
                position,
                Instant.now(),
                reason);
    }
}
