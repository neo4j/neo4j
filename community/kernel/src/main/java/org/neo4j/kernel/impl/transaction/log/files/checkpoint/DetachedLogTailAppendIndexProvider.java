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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import static org.neo4j.kernel.KernelVersion.VERSION_APPEND_INDEX_INTRODUCED;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;

import java.io.IOException;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.AppendBatchInfo;
import org.neo4j.kernel.impl.transaction.log.LastAppendBatchInfoProvider;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.AbstractLogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.UnsupportedLogVersionException;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkEnd;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkStart;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryRollback;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.storageengine.api.CommandReaderFactory;

public class DetachedLogTailAppendIndexProvider implements LastAppendBatchInfoProvider {
    private final LogFile logFile;
    private final KernelVersion kernelVersion;
    private final long startingAppendIndex;
    private final LogPosition logPosition;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;
    private final CommandReaderFactory commandReaderFactory;

    public DetachedLogTailAppendIndexProvider(
            CommandReaderFactory commandReaderFactory,
            BinarySupportedKernelVersions binarySupportedKernelVersions,
            LogFile logFile,
            KernelVersion kernelVersion,
            long startingAppendIndex,
            LogPosition logPosition) {
        this.logFile = logFile;
        this.kernelVersion = kernelVersion;
        this.startingAppendIndex = startingAppendIndex;
        this.logPosition = logPosition;
        this.binarySupportedKernelVersions = binarySupportedKernelVersions;
        this.commandReaderFactory = commandReaderFactory;
    }

    @Override
    public AppendBatchInfo get() {
        if (logPosition != null && logPosition != LogPosition.UNSPECIFIED) {
            long logVersion = logPosition.getLogVersion();
            boolean checkCommitEntries = kernelVersion.isLessThan(VERSION_APPEND_INDEX_INTRODUCED);
            long appendIndex = startingAppendIndex;
            LogPosition postLogPosition = logPosition;
            try {
                while (logFile.versionExists(logVersion)) {
                    var lookupPosition = getLookupPosition(logFile, logPosition, logVersion);
                    if (lookupPosition == LogPosition.UNSPECIFIED) {
                        // position to start lookup is unknown since we reached the file without header
                        return new AppendBatchInfo(appendIndex, postLogPosition);
                    }

                    var logEntryReader =
                            new VersionAwareLogEntryReader(commandReaderFactory, binarySupportedKernelVersions);
                    try (var reader = logFile.getReader(lookupPosition, NO_MORE_CHANNELS);
                            var cursor = new LogEntryCursor(logEntryReader, reader)) {
                        AbstractLogEntry entry;
                        while (cursor.next()) {
                            entry = (AbstractLogEntry) cursor.get();
                            if (entry instanceof LogEntryStart startEntry) {
                                appendIndex = startEntry.getAppendIndex();
                            } else if (entry instanceof LogEntryChunkStart chunkStart) {
                                appendIndex = chunkStart.getAppendIndex();
                            } else if (entry instanceof LogEntryChunkEnd) {
                                postLogPosition = reader.getCurrentLogPosition();
                            } else if (entry instanceof LogEntryRollback rollback) {
                                appendIndex = rollback.getAppendIndex();
                                postLogPosition = reader.getCurrentLogPosition();
                            } else if ((entry instanceof LogEntryCommit commit)) {
                                if (checkCommitEntries) {
                                    appendIndex = commit.getTxId();
                                }
                                postLogPosition = reader.getCurrentLogPosition();
                            }
                        }
                    } catch (IOException | IllegalStateException | UnsupportedLogVersionException e) {
                        // error on reading log file returning last known existing
                        return new AppendBatchInfo(appendIndex, postLogPosition);
                    }
                    logVersion++;
                }
                return new AppendBatchInfo(appendIndex, postLogPosition);
            } catch (Throwable t) {
                throw new RuntimeException("Unable to retrieve last append index", t);
            }
        }
        return new AppendBatchInfo(startingAppendIndex, LogPosition.UNSPECIFIED);
    }

    private static LogPosition getLookupPosition(LogFile logFile, LogPosition logPosition, long logVersion) {
        if (logVersion == logPosition.getLogVersion()) {
            return logPosition;
        }
        try {
            LogHeader logHeader = logFile.extractHeader(logVersion);
            if (logHeader != null) {
                return logHeader.getStartPosition();
            }
        } catch (IOException e) {
            return LogPosition.UNSPECIFIED;
        }
        return LogPosition.UNSPECIFIED;
    }
}
