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

import java.io.IOException;
import java.util.function.Supplier;
import org.neo4j.internal.helpers.collection.LfuCache;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.kernel.impl.transaction.log.EnvelopeReadChannel;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkStart;
import org.neo4j.util.VisibleForTesting;

public class TransactionLogFileInformation implements LogFileInformation {
    private final LogFiles logFiles;
    private final LogHeaderCache logHeaderCache;
    private final TransactionLogFilesContext logFileContext;
    private final TransactionLogFileTimestampMapper logFileTimestampMapper;

    TransactionLogFileInformation(
            LogFiles logFiles, LogHeaderCache logHeaderCache, TransactionLogFilesContext context) {
        this(
                logFiles,
                logHeaderCache,
                context,
                () -> new VersionAwareLogEntryReader(
                        context.getCommandReaderFactory(), context.getBinarySupportedKernelVersions()));
    }

    @VisibleForTesting
    TransactionLogFileInformation(
            LogFiles logFiles,
            LogHeaderCache logHeaderCache,
            TransactionLogFilesContext context,
            Supplier<LogEntryReader> logEntryReaderFactory) {
        this.logFiles = logFiles;
        this.logHeaderCache = logHeaderCache;
        this.logFileContext = context;
        this.logFileTimestampMapper = new TransactionLogFileTimestampMapper(logFiles, logEntryReaderFactory);
    }

    @Override
    public long getFirstExistingEntryId() throws IOException {
        LogFile logFile = logFiles.getLogFile();
        long version = logFile.getHighestLogVersion();
        long candidateFirstTx = -1;
        while (logFile.versionExists(version)) {
            candidateFirstTx = getFirstEntryId(version);
            version--;
        }
        version++; // the loop above goes back one version too far.

        // OK, so we now have the oldest existing log version here. Open it and see if there's any transaction
        // in there. If there is then that transaction is the first one that we have.
        return logFile.hasAnyEntries(version) ? candidateFirstTx : -1;
    }

    @Override
    public long getFirstEntryId(long version) throws IOException {
        LogHeader logHeader = logHeaderCache.getLogHeader(version);
        if (logHeader != null) { // It existed in cache
            return logHeader.getLastCommittedTxId() + 1;
        }

        // Wasn't cached, go look for it
        var logFile = logFiles.getLogFile();
        if (logFile.versionExists(version)) {
            logHeader = logFile.extractHeader(version);
            if (logHeader != null) {
                logHeaderCache.putHeader(version, logHeader);
                return logHeader.getLastCommittedTxId() + 1;
            }
        }
        return -1;
    }

    @Override
    public long getLastEntryId() {
        return logFileContext.getLastCommittedTransactionIdProvider().getLastCommittedTransactionId(logFiles);
    }

    @Override
    public long committingEntryId() {
        return logFileContext.committingTransactionId();
    }

    @Override
    public long getFirstStartRecordTimestamp(long version) throws IOException {
        return logFileTimestampMapper.getTimestampForVersion(version);
    }

    private static class TransactionLogFileTimestampMapper {
        private static final String FIRST_TRANSACTION_TIME = "First Transaction Time";
        private final LogFiles logFiles;
        private final Supplier<LogEntryReader> logEntryReaderFactory;
        private final LfuCache<Long, Long> logFileTimeStamp = new LfuCache<>(FIRST_TRANSACTION_TIME, 10_000);

        TransactionLogFileTimestampMapper(LogFiles logFiles, Supplier<LogEntryReader> logEntryReaderFactory) {
            this.logFiles = logFiles;
            this.logEntryReaderFactory = logEntryReaderFactory;
        }

        long getTimestampForVersion(long version) throws IOException {
            var cachedTimeStamp = logFileTimeStamp.get(version);
            if (cachedTimeStamp != null) {
                return cachedTimeStamp;
            }
            var logFile = logFiles.getLogFile();
            if (logFile.versionExists(version)) {
                LogHeader logHeader = logFile.extractHeader(version);
                if (logHeader != null) {
                    LogPosition position = logHeader.getStartPosition();
                    try (ReadableLogChannel channel = logFile.getRawReader(position)) {
                        if (logHeader.getLogFormatVersion().usesSegments()) {
                            try {
                                // Make sure we look at the beginning of a transaction
                                ((EnvelopeReadChannel) channel).goToNextEntry();
                            } catch (ReadPastEndException e) {
                                // If there was no start/full envelopes in the file we could reach the end
                                return -1;
                            }
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
