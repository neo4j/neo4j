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
package org.neo4j.kernel.impl.transaction.log.pruning;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;
import org.neo4j.internal.helpers.collection.LfuCache;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryChunkStart;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.util.VisibleForTesting;

/**
 * Builder of per-version {@link LogFileInformation} views over a {@link LogFiles}. Each {@link #forVersion} call
 * returns a thin handle that delegates back into the shared LFU timestamp cache and the log file's header reader.
 */
public class TransactionLogFileInformation {
    private final LogFiles logFiles;
    private final TimestampMapper timestampMapper;

    TransactionLogFileInformation(
            LogFiles logFiles,
            CommandReaderFactory commandReaderFactory,
            BinarySupportedKernelVersions binarySupportedKernelVersions,
            MemoryTracker memoryTracker) {
        this(
                logFiles,
                () -> new VersionAwareLogEntryReader(
                        commandReaderFactory, binarySupportedKernelVersions, memoryTracker));
    }

    @VisibleForTesting
    TransactionLogFileInformation(LogFiles logFiles, Supplier<LogEntryReader> logEntryReaderFactory) {
        this.logFiles = logFiles;
        this.timestampMapper = new TimestampMapper(logFiles, logEntryReaderFactory);
    }

    public LogFileInformation forVersion(long version) {
        return new VersionView(version);
    }

    private final class VersionView implements LogFileInformation {
        private final long version;

        private VersionView(long version) {
            this.version = version;
        }

        @Override
        public long version() {
            return version;
        }

        @Override
        public Path path() {
            return logFiles.getLogFile().getLogFileForVersion(version);
        }

        @Override
        public long getPreviousAppendIndexFromHeader() throws IOException {
            LogHeader header = logFiles.getLogFile().extractHeader(version);
            return header != null ? header.getLastAppendIndex() : -1;
        }

        @Override
        public long getFirstStartRecordTimestamp() throws IOException {
            return timestampMapper.getTimestampForVersion(version);
        }
    }

    private static class TimestampMapper {
        private static final String FIRST_TRANSACTION_TIME = "First Transaction Time";
        private final LogFiles logFiles;
        private final Supplier<LogEntryReader> logEntryReaderFactory;
        private final LfuCache<Long, Long> logFileTimeStamp = new LfuCache<>(FIRST_TRANSACTION_TIME, 10_000);

        TimestampMapper(LogFiles logFiles, Supplier<LogEntryReader> logEntryReaderFactory) {
            this.logFiles = logFiles;
            this.logEntryReaderFactory = logEntryReaderFactory;
        }

        long getTimestampForVersion(long version) throws IOException {
            var cached = logFileTimeStamp.get(version);
            if (cached != null) {
                return cached;
            }
            var logFile = logFiles.getLogFile();
            if (logFile.versionExists(version)) {
                LogHeader logHeader = logFile.extractHeader(version);
                if (logHeader != null) {
                    LogPosition position = logHeader.getStartPosition();
                    try (ReadableLogChannel channel = logFile.getRawReader(position)) {
                        try {
                            // Make sure we look at the beginning of a transaction
                            channel.alignWithStartEntry();
                        } catch (ReadPastEndException e) {
                            // If there was no start/full envelopes in the file we could reach the end
                            return -1;
                        }
                        var logEntryReader = logEntryReaderFactory.get();
                        LogEntry entry;
                        while ((entry = logEntryReader.readLogEntry(channel)) != null) {
                            if (entry instanceof LogEntryStart logEntryStart) {
                                return cacheTimeWritten(version, logEntryStart.getTimeWritten());
                            } else if (entry instanceof LogEntryChunkStart chunkStart) {
                                return cacheTimeWritten(version, chunkStart.getTimeWritten());
                            }
                        }
                    }
                }
            }
            return -1;
        }

        private long cacheTimeWritten(long version, long timeWritten) {
            logFileTimeStamp.put(version, timeWritten);
            return timeWritten;
        }
    }
}
