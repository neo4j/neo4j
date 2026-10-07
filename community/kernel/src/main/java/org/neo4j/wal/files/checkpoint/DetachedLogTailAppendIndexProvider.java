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
package org.neo4j.wal.files.checkpoint;

import static java.lang.Math.max;
import static org.neo4j.kernel.KernelVersion.VERSION_APPEND_INDEX_INTRODUCED;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.wal.LogIndexEncoding.decodeLogIndex;

import java.io.IOException;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.wal.AppendBatchInfo;
import org.neo4j.wal.LastAppendBatchInfoProvider;
import org.neo4j.wal.LogEntryCursor;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.ReadableLogChannel;
import org.neo4j.wal.ReaderLogVersionBridge;
import org.neo4j.wal.entry.LogEntryCommit;
import org.neo4j.wal.entry.LogEntryEmpty;
import org.neo4j.wal.entry.LogEntryStart;
import org.neo4j.wal.entry.LogHeader;
import org.neo4j.wal.entry.UnsupportedLogVersionException;
import org.neo4j.wal.entry.VersionAwareLogEntryReader;
import org.neo4j.wal.entry.v520.LogEntryChunkEnd;
import org.neo4j.wal.entry.v520.LogEntryChunkStart;
import org.neo4j.wal.entry.v520.LogEntryRollback;

public class DetachedLogTailAppendIndexProvider implements LastAppendBatchInfoProvider {
    private final LogFile logFile;
    private final KernelVersion kernelVersion;
    private final long startingAppendIndex;
    private final LogPosition logPosition;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;
    private final CommandReaderFactory commandReaderFactory;
    private final MemoryTracker memoryTracker;
    private final LogPosition maxPosition;
    private final long startingConsensusIndex;
    private final boolean failOnUnsupportedLogVersion;

    public DetachedLogTailAppendIndexProvider(
            CommandReaderFactory commandReaderFactory,
            BinarySupportedKernelVersions binarySupportedKernelVersions,
            LogFile logFile,
            KernelVersion kernelVersion,
            long startingAppendIndex,
            long startingConsensusIndex,
            LogPosition logPosition,
            MemoryTracker memoryTracker,
            LogPosition maxPosition,
            boolean failOnUnsupportedLogVersion) {
        this.logFile = logFile;
        this.kernelVersion = kernelVersion;
        this.startingAppendIndex = startingAppendIndex;
        this.startingConsensusIndex = startingConsensusIndex;
        if (logPosition != LogPosition.UNSPECIFIED
                && maxPosition != LogPosition.UNSPECIFIED
                && maxPosition.isBefore(logPosition)) {
            throw new IllegalStateException(
                    "checkpoint start position=" + logPosition + " is after maxPosition=" + maxPosition);
        }
        this.logPosition = logPosition;
        this.binarySupportedKernelVersions = binarySupportedKernelVersions;
        this.commandReaderFactory = commandReaderFactory;
        this.memoryTracker = memoryTracker;
        this.maxPosition = maxPosition;
        this.failOnUnsupportedLogVersion = failOnUnsupportedLogVersion;
    }

    @Override
    public AppendBatchInfo get() {
        if (logPosition == null || logPosition == LogPosition.UNSPECIFIED) {
            return new AppendBatchInfo(startingAppendIndex, LogPosition.UNSPECIFIED, startingConsensusIndex);
        }
        long logVersion = logPosition.getLogVersion();
        boolean checkCommitEntries = kernelVersion.isLessThan(VERSION_APPEND_INDEX_INTRODUCED);
        long appendIndex = startingAppendIndex;
        long consensusIndex = startingConsensusIndex;
        LogPosition postLogPosition = logPosition;
        try {
            if (!logFile.versionExists(logVersion)) {
                return new AppendBatchInfo(appendIndex, postLogPosition, consensusIndex);
            }
            var logEntryReader =
                    new VersionAwareLogEntryReader(commandReaderFactory, binarySupportedKernelVersions, memoryTracker);
            long currentFileVersion = logFile.getLogRangeInfo().highestVersion();
            if (maxPosition != LogPosition.UNSPECIFIED && currentFileVersion > maxPosition.getLogVersion()) {
                currentFileVersion = maxPosition.getLogVersion();
            }

            while (currentFileVersion >= logVersion) {
                long currentAppendIndex = UNKNOWN_APPEND_INDEX;
                long currentConsensusIndex = UNKNOWN_CONSENSUS_INDEX;
                boolean infoFound = false;
                LogPosition logFileStartPosition = getLogFileStartPosition(currentFileVersion, logVersion);
                // if file does not even have header lets switch to the previous one
                if (logFileStartPosition != null) {
                    // Header exist (and is cached) since we were able to figure out logFileStartPosition
                    LogHeader logHeader = logFile.extractHeader(currentFileVersion);
                    if (appendIndex == UNKNOWN_APPEND_INDEX) {
                        // let's initiate appendIndex to the header last append index just in case we don't find any
                        // entries at all. The max prevents setting to -1 in merge logs
                        appendIndex = max(logHeader.getLastAppendIndex(), appendIndex);
                        if (logHeader.getLogFormatVersion().supportsConsensusEntries()) {
                            consensusIndex = appendIndex;
                        }
                    }
                    try (var reader =
                            logFile.getReader(logFileStartPosition, ReaderLogVersionBridge.forFile(logFile))) {
                        if (startingAppendIndex == UNKNOWN_APPEND_INDEX
                                && logHeader.getLogFormatVersion().usesSegments()) {
                            // the header only knows the index at the start of the file, so load the index of the
                            // last entry before the position in case there are no entries after it
                            reader.reloadChannelStateBeforeCurrentPosition();
                            appendIndex = max(reader.getAppendIndex(), appendIndex);
                            if (logHeader.getLogFormatVersion().supportsConsensusEntries()) {
                                consensusIndex = appendIndex;
                            }
                        }
                        if (isAfterOrSameAsMaxPosition(reader)) {
                            currentFileVersion--;
                            continue;
                        }
                        reader.alignWithStartEntry();
                        if (isAfterOrSameAsMaxPosition(reader)) {
                            currentFileVersion--;
                            continue;
                        }
                        try (var cursor = new LogEntryCursor(logEntryReader, reader)) {
                            while (cursor.next()) {
                                // If we are already past the max position then drop out
                                // and possibly go back another version, but do process
                                // if we have only just reached the maxPosition though
                                if (isAfterMaxPosition(reader)) {
                                    break;
                                }
                                var entry = cursor.get();
                                if (entry instanceof LogEntryStart startEntry) {
                                    currentAppendIndex = startEntry.getAppendIndex();
                                    currentConsensusIndex = decodeLogIndexOrUnknown(startEntry.getAdditionalHeader());
                                } else if (entry instanceof LogEntryChunkStart chunkStart) {
                                    currentAppendIndex = chunkStart.getAppendIndex();
                                    currentConsensusIndex = decodeLogIndexOrUnknown(chunkStart.getAdditionalHeader());
                                } else if (entry instanceof LogEntryRollback rollback) {
                                    appendIndex = rollback.getAppendIndex();
                                    consensusIndex = rollback.getConsensusIndex();
                                    postLogPosition = reader.getCurrentLogPosition();
                                    infoFound = true;
                                } else if (entry instanceof LogEntryChunkEnd || (entry instanceof LogEntryCommit)) {
                                    if (checkCommitEntries && entry instanceof LogEntryCommit commit) {
                                        currentAppendIndex = commit.getTxId();
                                    }
                                    appendIndex = currentAppendIndex;
                                    consensusIndex = currentConsensusIndex;
                                    postLogPosition = reader.getCurrentLogPosition();
                                    currentAppendIndex = UNKNOWN_APPEND_INDEX;
                                    currentConsensusIndex = UNKNOWN_CONSENSUS_INDEX;
                                    infoFound = true;
                                } else if (entry instanceof LogEntryEmpty empty) {
                                    appendIndex = empty.getAppendIndex();
                                    consensusIndex = empty.getAppendIndex(); // must be equal in merge logs
                                    postLogPosition = reader.getCurrentLogPosition();
                                    infoFound = true;
                                }
                                // don't attempt any further read beyond maxPosition
                                if (isAfterOrSameAsMaxPosition(reader)) {
                                    break;
                                }
                            }
                            if (infoFound) {
                                return new AppendBatchInfo(appendIndex, postLogPosition, consensusIndex);
                            }
                        }
                    } catch (UnsupportedLogVersionException e) {
                        if (failOnUnsupportedLogVersion) {
                            throw e;
                        }
                        return new AppendBatchInfo(appendIndex, postLogPosition, consensusIndex);
                    } catch (IOException | IllegalStateException e) {
                        // error on reading log file returning last known existing
                        return new AppendBatchInfo(appendIndex, postLogPosition, consensusIndex);
                    }
                }
                currentFileVersion--;
            }
            return new AppendBatchInfo(appendIndex, postLogPosition, consensusIndex);
        } catch (UnsupportedLogVersionException e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException("Unable to retrieve last append index", t);
        }
    }

    private static long decodeLogIndexOrUnknown(byte[] additionalHeader) {
        try {
            return decodeLogIndex(additionalHeader);
        } catch (IllegalArgumentException e) {
            return UNKNOWN_CONSENSUS_INDEX;
        }
    }

    private boolean isAfterOrSameAsMaxPosition(ReadableLogChannel reader) throws IOException {
        return maxPosition != LogPosition.UNSPECIFIED
                && reader.getCurrentLogPosition().isAfterOrSame(maxPosition);
    }

    private boolean isAfterMaxPosition(ReadableLogChannel reader) throws IOException {
        return maxPosition != LogPosition.UNSPECIFIED && maxPosition.isBefore(reader.getCurrentLogPosition());
    }

    private LogPosition getLogFileStartPosition(long currentFileVersion, long logVersion) throws IOException {
        if (currentFileVersion == logVersion) {
            return logPosition;
        }
        LogHeader logHeader = logFile.extractHeader(currentFileVersion);
        if (logHeader == null) {
            return null;
        }
        return logHeader.getStartPosition();
    }
}
