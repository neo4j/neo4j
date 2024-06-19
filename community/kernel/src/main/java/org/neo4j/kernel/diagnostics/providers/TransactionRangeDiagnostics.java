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
package org.neo4j.kernel.diagnostics.providers;

import static org.neo4j.io.ByteUnit.bytesToString;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.diagnostics.NamedDiagnosticsProvider;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.Format;
import org.neo4j.io.device.DeviceMapper;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.logging.NullLog;
import org.neo4j.storageengine.api.TransactionIdStore;

public class TransactionRangeDiagnostics extends NamedDiagnosticsProvider {
    private final Database database;

    TransactionRangeDiagnostics(Database database) {
        super("Transaction log");
        this.database = database;
    }

    @Override
    public void dump(DiagnosticsLogger logger) {
        final var dependencyResolver = database.getDependencyResolver();
        final var deviceMapper = dependencyResolver.resolveDependency(DeviceMapper.class);
        final var kernelVersionProvider = dependencyResolver.resolveDependency(KernelVersionProvider.class);
        final var txIdStore = dependencyResolver.resolveDependency(TransactionIdStore.class);
        final var logFiles = dependencyResolver.resolveDependency(LogFiles.class);
        final var fileSystem = dependencyResolver.resolveDependency(FileSystemAbstraction.class);

        try {
            logger.log("Transaction log files stored on file store: "
                    + deviceMapper.describePath(logFiles.logFilesDirectory()));
            dumpTransactionLogMetadata(logger, kernelVersionProvider, txIdStore);
            dumpTransactionLogInformation(logger, logFiles.getLogFile(), fileSystem);
            dumpCheckpointLogInformation(logger, logFiles.getCheckpointFile());
        } catch (Exception e) {
            logger.log("Error trying to dump transaction log files info.");
            logger.log(Exceptions.stringify(e));
        }
    }

    private void dumpTransactionLogMetadata(
            DiagnosticsLogger logger, KernelVersionProvider kernelVersionProvider, TransactionIdStore txIdStore) {
        logger.log("Transaction log metadata:");
        logger.log(" - current kernel version used in transactions: "
                + kernelVersionProvider.kernelVersion().name());
        logger.log(" - last committed transaction id: " + txIdStore.getLastCommittedTransactionId());
    }

    private void dumpTransactionLogInformation(
            DiagnosticsLogger logger, LogFile logFile, FileSystemAbstraction fileSystem) throws IOException {
        logger.log("Transaction log files:");
        logger.log(" - existing transaction log versions: " + logFile.getLowestLogVersion() + "-"
                + logFile.getHighestLogVersion());
        boolean foundTransactions = false;
        for (long logVersion = logFile.getLowestLogVersion();
                logFile.versionExists(logVersion) && !foundTransactions;
                logVersion++) {
            if (logFile.hasAnyEntries(logVersion)) {
                LogHeader header = logFile.extractHeader(logVersion);
                long firstAppendIndexInThisLog = header.getLastAppendIndex() + 1;
                logger.log(" - oldest append index " + firstAppendIndexInThisLog + " found in log with version "
                        + logVersion);
                foundTransactions = true;
            }
        }
        if (!foundTransactions) {
            logger.log(" - no transactions found");
        } else {
            logger.log(" - files: (filename : creation date - size)");
            long totalSize = 0;
            for (Path txLogFile : logFile.getMatchedFiles()) {
                long size = fileSystem.getFileSize(txLogFile);
                totalSize += size;
                logger.log(String.format(
                        "     %s: %s - %s",
                        txLogFile.getFileName(), getFileCreationDate(txLogFile), bytesToString(size)));
            }
            logger.log(" - total size of files: " + bytesToString(totalSize));
        }
    }

    private void dumpCheckpointLogInformation(DiagnosticsLogger logger, CheckpointFile checkpointFile)
            throws IOException {
        logger.log("Checkpoint log files:");
        logger.log(" - existing checkpoint log versions: " + checkpointFile.getLowestLogVersion() + "-"
                + checkpointFile.getHighestLogVersion());
        checkpointFile
                .findLatestCheckpoint(NullLog.getInstance())
                .ifPresentOrElse(
                        checkpoint -> logger.log(" - last checkpoint: " + checkpoint),
                        () -> logger.log(" - no checkpoints found"));
    }

    private static String getFileCreationDate(Path file) {
        try {
            Instant instant = Files.readAttributes(file, BasicFileAttributes.class)
                    .creationTime()
                    .toInstant();
            return Format.date(instant);
        } catch (IOException e) {
            return "<UNKNOWN>";
        }
    }
}
