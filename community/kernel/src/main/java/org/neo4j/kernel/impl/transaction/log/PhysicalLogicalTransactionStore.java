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

import static org.neo4j.configuration.GraphDatabaseInternalSettings.pre_sketch_transaction_logs;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.reverse.ForwardCommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.reverse.ReversedMultiFileCommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.reverse.ReversedTransactionCursorMonitor;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.CommandReaderFactory;

public class PhysicalLogicalTransactionStore implements LogicalTransactionStore {
    private final LogFile logFile;
    private final TransactionMetadataCache transactionMetadataCache;
    private final CommandReaderFactory commandReaderFactory;
    private final Monitors monitors;
    private final boolean failOnCorruptedLogFiles;
    private final boolean presketchLogFiles;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;
    private final FileSystemAbstraction fs;

    public PhysicalLogicalTransactionStore(
            LogFiles logFiles,
            TransactionMetadataCache transactionMetadataCache,
            CommandReaderFactory commandReaderFactory,
            Monitors monitors,
            boolean failOnCorruptedLogFiles,
            Config config,
            FileSystemAbstraction fs) {
        this.logFile = logFiles.getLogFile();
        this.transactionMetadataCache = transactionMetadataCache;
        this.commandReaderFactory = commandReaderFactory;
        this.monitors = monitors;
        this.failOnCorruptedLogFiles = failOnCorruptedLogFiles;
        this.presketchLogFiles = config.get(pre_sketch_transaction_logs);
        this.binarySupportedKernelVersions = new BinarySupportedKernelVersions(config);
        this.fs = fs;
    }

    @Override
    public CommandBatchCursor getCommandBatches(LogPosition position) throws IOException {
        return new ForwardCommandBatchCursor(
                logFile,
                position,
                new VersionAwareLogEntryReader(commandReaderFactory, binarySupportedKernelVersions),
                fs);
    }

    @Override
    public CommandBatchCursor getCommandBatchesInReverseOrder(LogPosition backToPosition) {
        return ReversedMultiFileCommandBatchCursor.fromLogFile(
                logFile,
                backToPosition,
                new VersionAwareLogEntryReader(commandReaderFactory, binarySupportedKernelVersions),
                failOnCorruptedLogFiles,
                monitors.newMonitor(ReversedTransactionCursorMonitor.class),
                presketchLogFiles,
                false);
    }

    @Override
    public CommandBatchCursor getCommandBatches(final long appendIndexToStartFrom) throws IOException {
        // look up in position cache
        try {
            var logEntryReader = new VersionAwareLogEntryReader(commandReaderFactory, binarySupportedKernelVersions);
            TransactionMetadataCache.TransactionMetadata transactionMetadata =
                    transactionMetadataCache.getTransactionMetadata(appendIndexToStartFrom);
            if (transactionMetadata != null) {
                // we're good
                return new ForwardCommandBatchCursor(logFile, transactionMetadata.startPosition(), logEntryReader, fs);
            }

            // ask logFiles about the version it may be in
            var headerVisitor = new AppendedChunkLogVersionLocator(appendIndexToStartFrom);
            logFile.accept(headerVisitor);

            // ask LogFile
            var transactionPositionLocator = new AppendedChunkPositionLocator(appendIndexToStartFrom, logEntryReader);
            logFile.accept(transactionPositionLocator, headerVisitor.getLogPositionOrThrow());
            var position = transactionPositionLocator.getLogPositionOrThrow();
            transactionMetadataCache.cacheTransactionMetadata(appendIndexToStartFrom, position);
            return new ForwardCommandBatchCursor(logFile, position, logEntryReader, fs);
        } catch (NoSuchFileException e) {
            throw new NoSuchLogEntryException(
                    appendIndexToStartFrom,
                    "Log position acquired, but couldn't find the log file itself. Perhaps it just recently was "
                            + "deleted? [" + e.getMessage() + "]",
                    e);
        }
    }
}
