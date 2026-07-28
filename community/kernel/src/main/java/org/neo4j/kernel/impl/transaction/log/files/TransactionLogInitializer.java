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
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CHUNK_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch;
import org.neo4j.kernel.impl.transaction.log.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.storageengine.api.LogMetadataProvider;
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
    private final LogMetadataProvider logMetadataProvider;
    private final StorageEngineFactory storageEngineFactory;

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
                    LogMetadataProvider logMetadataProvider,
                    FileSystemAbstraction fileSystem,
                    String checkpointReason,
                    Config config) {
                try {
                    TransactionLogInitializer initializer = new TransactionLogInitializer(
                            fileSystem,
                            metadataProvider,
                            logMetadataProvider,
                            StorageEngineFactory.selectStorageEngine(fileSystem, databaseLayout)
                                    .orElseThrow());
                    initializer.initializeEmptyLogFile(
                            databaseLayout, databaseLayout.getTransactionLogsDirectory(), checkpointReason, config);
                } catch (IOException e) {
                    throw new UnderlyingStorageException("Failed to initialize transaction log files.", e);
                }
            }

            @Override
            public void clearHistoryAndInitializeLogFiles(
                    DatabaseLayout databaseLayout,
                    MetadataProvider metadataProvider,
                    LogMetadataProvider logMetadataProvider,
                    FileSystemAbstraction fileSystem,
                    String checkpointReason) {
                try {
                    TransactionLogInitializer initializer = new TransactionLogInitializer(
                            fileSystem,
                            metadataProvider,
                            logMetadataProvider,
                            StorageEngineFactory.selectStorageEngine(fileSystem, databaseLayout)
                                    .orElseThrow());
                    initializer.migrateExistingLogFiles(
                            databaseLayout,
                            databaseLayout.getTransactionLogsDirectory(),
                            checkpointReason,
                            Config.defaults());
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
            LogMetadataProvider logMetadataProvider,
            StorageEngineFactory storageEngineFactory) {
        this.fs = fs;
        this.metadataProvider = metadataProvider;
        this.logMetadataProvider = logMetadataProvider;
        this.storageEngineFactory = storageEngineFactory;
    }

    /**
     * Create new empty log files in the given transaction logs directory, for a database that doesn't have any already.
     */
    public long initializeEmptyLogFile(
            DatabaseLayout layout, Path transactionLogsDirectory, String checkpointReason, Config config)
            throws IOException {
        try (LogFilesSpan span = buildLogFiles(layout, transactionLogsDirectory, config)) {
            LogFiles logFiles = span.getLogFiles();
            return appendEmptyTransactionAndCheckPoint(logFiles, checkpointReason);
        }
    }

    public long migrateExistingLogFiles(
            DatabaseLayout layout, Path transactionLogsDirectory, String checkpointReason, Config config)
            throws Exception {
        try (LogFilesSpan span = buildLogFiles(layout, transactionLogsDirectory, config)) {
            LogFiles logFiles = span.getLogFiles();
            LogFile logFile = logFiles.getLogFile();
            LogRangeInfo logRangeInfo = logFile.getLogRangeInfo();
            for (long version = logRangeInfo.lowestVersion(); version <= logRangeInfo.highestVersion(); version++) {
                fs.deleteFile(logFile.getLogFileForVersion(version));
            }
            CheckpointFile checkpointFile = logFiles.getCheckpointFile();
            LogRangeInfo checkpointLogVersionRange = checkpointFile.getLogRangeInfo();
            for (long version = checkpointLogVersionRange.lowestVersion();
                    version <= checkpointLogVersionRange.highestVersion();
                    version++) {
                fs.deleteFile(checkpointFile.getLogFileForVersion(version));
            }
            logFile.rotate();
            checkpointFile.rotate();
            return appendEmptyTransactionAndCheckPoint(logFiles, checkpointReason);
        }
    }

    private LogFilesSpan buildLogFiles(DatabaseLayout layout, Path transactionLogsDirectory, Config config)
            throws IOException {
        LogFilesBuilder builder = LogFilesBuilder.writeableBuilder(layout, fs, logMetadataProvider, logMetadataProvider)
                .withLogVersionRepository(logMetadataProvider)
                .withTransactionIdStore(logMetadataProvider)
                .withAppendIndexProvider(logMetadataProvider)
                .withLogTermProvider(logMetadataProvider)
                .withStoreId(metadataProvider.getStoreId())
                .withLogsDirectory(transactionLogsDirectory)
                .withStorageEngineFactory(storageEngineFactory)
                .withConfig(config)
                .withDatabaseHealth(new DatabaseHealth(HealthEventGenerator.NO_OP, NullLog.getInstance()))
                .withLogFormatVersionProvider(logMetadataProvider)
                .withKernelVersionProvider(logMetadataProvider);
        if (logMetadataProvider.kernelVersion() == KernelVersion.GLORIOUS_FUTURE) {
            builder.withConfig(Config.defaults(
                    GraphDatabaseInternalSettings.latest_kernel_version,
                    logMetadataProvider.kernelVersion().version()));
        }
        LogFiles logFiles = builder.build();
        return new LogFilesSpan(new Lifespan(logFiles), logFiles);
    }

    private long appendEmptyTransactionAndCheckPoint(LogFiles logFiles, String reason) throws IOException {
        TransactionId committedTx = logMetadataProvider.getLastCommittedTransaction();
        long consensusIndex = UNKNOWN_CONSENSUS_INDEX;
        long timestamp = committedTx.commitTimestamp();
        long upgradeTransactionId = logMetadataProvider.nextCommittingTransactionId();
        long appendIndex = logMetadataProvider.nextAppendIndex();
        KernelVersion kernelVersion = logMetadataProvider.kernelVersion();
        LogFile logFile = logFiles.getLogFile();
        TransactionLogWriter transactionLogWriter = logFile.getTransactionLogWriter();
        CompleteCommandBatch emptyTx = emptyTransaction(timestamp, upgradeTransactionId, kernelVersion, consensusIndex);
        int checksum = transactionLogWriter.append(
                emptyTx,
                upgradeTransactionId,
                UNKNOWN_CHUNK_ID,
                appendIndex,
                BASE_TX_CHECKSUM,
                UNKNOWN_APPEND_INDEX,
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
        logMetadataProvider.transactionCommitted(
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
                Leases.NO_LEASES,
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
