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

import static java.lang.Math.min;
import static java.lang.Math.subtractExact;
import static java.lang.String.format;
import static org.neo4j.internal.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.fs.FileUtils.getCanonicalFile;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.files.RangeLogVersionVisitor.UNKNOWN;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogIndexEncoding;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.UnsupportedLogVersionException;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkEnd;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogTailInformation;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;
import org.neo4j.kernel.recovery.LogTailScannerMonitor;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;

public class DetachedLogTailScanner {
    static final long NO_TRANSACTION_ID = -1;
    public static final byte NO_ENTRY = 0;
    private static final String TRANSACTION_LOG_NAME = "Transaction";
    private static final String CHECKPOINT_LOG_NAME = "Checkpoint";
    private final LogFiles logFiles;
    private final CommandReaderFactory commandReaderFactory;
    private final LogTailScannerMonitor monitor;
    private final MemoryTracker memoryTracker;
    private final CheckpointFile checkpointFile;
    private final boolean failOnCorruptedLogFiles;
    private final FileSystemAbstraction fileSystem;
    private final KernelVersionProvider fallbackKernelVersionProvider;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;

    private LogTailMetadata logTail;

    public DetachedLogTailScanner(
            LogFiles logFiles,
            TransactionLogFilesContext context,
            CheckpointFile checkpointFile,
            LogTailScannerMonitor monitor) {
        this.logFiles = logFiles;
        this.commandReaderFactory = context.getCommandReaderFactory();
        this.memoryTracker = context.getMemoryTracker();
        this.checkpointFile = checkpointFile;
        this.fileSystem = context.getFileSystem();
        this.failOnCorruptedLogFiles = context.isFailOnCorruptedLogFiles();
        this.fallbackKernelVersionProvider = context.getKernelVersionProvider();
        this.logTail = context.getExternalTailInfo();
        this.monitor = monitor;
        this.binarySupportedKernelVersions = context.getBinarySupportedKernelVersions();
    }

    public LogTailInformation findLogTail() {
        LogFile logFile = logFiles.getLogFile();
        long highestLogVersion = logFile.getHighestLogVersion();
        long lowestLogVersion = logFile.getLowestLogVersion();
        try {
            var lastAccessibleCheckpoint = checkpointFile.findLatestCheckpoint();
            if (lastAccessibleCheckpoint.isEmpty()) {
                return noCheckpointLogTail(logFile, highestLogVersion, lowestLogVersion);
            }
            var checkpoint = lastAccessibleCheckpoint.get();
            verifyKernelVersion(checkpoint.kernelVersionByte());
            verifyCheckpointPosition(checkpoint.channelPositionAfterCheckpoint());
            // found checkpoint pointing to existing position in existing log file
            if (isValidCheckpoint(logFile, checkpoint)) {
                return validCheckpointLogTail(logFile, highestLogVersion, lowestLogVersion, checkpoint);
            }
            if (failOnCorruptedLogFiles) {
                var exceptionMessage = format(
                        "Last available %s checkpoint does not point to a valid location in transaction logs.",
                        checkpoint);
                throwUnableToCleanRecover(new RuntimeException(exceptionMessage));
            }
            // our last checkpoint is not valid (we have a pointer to non existent place) lets try to find last one that
            // looks correct
            List<CheckpointInfo> checkpointInfos = checkpointFile.reachableCheckpoints();
            // we know that last one is not valid so no reason to double check that again
            ListIterator<CheckpointInfo> reverseCheckpoints = checkpointInfos.listIterator(checkpointInfos.size() - 1);
            while (reverseCheckpoints.hasPrevious()) {
                CheckpointInfo previousCheckpoint = reverseCheckpoints.previous();
                if (isValidCheckpoint(logFile, previousCheckpoint)) {
                    return validCheckpointLogTail(logFile, highestLogVersion, lowestLogVersion, previousCheckpoint);
                }
            }
            // we did not found any valid, we need to restore from the start if possible
            return noCheckpointLogTail(logFile, highestLogVersion, lowestLogVersion);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void verifyKernelVersion(byte kernelVersionByte) {
        KernelVersion kernelVersion;
        try {
            kernelVersion = KernelVersion.getForVersion(kernelVersionByte);
        } catch (IllegalArgumentException e) {
            throw UnsupportedLogVersionException.unsupported(binarySupportedKernelVersions, kernelVersionByte);
        }

        if (!binarySupportedKernelVersions.latestSupportedIsAtLeast(kernelVersion)) {
            throw UnsupportedLogVersionException.unsupported(binarySupportedKernelVersions, kernelVersionByte);
        }
    }

    private LogTailInformation validCheckpointLogTail(
            LogFile logFile, long highestLogVersion, long lowestLogVersion, CheckpointInfo checkpoint)
            throws IOException {
        var entries = getFirstTransactionIdAfterCheckpoint(logFile, checkpoint.transactionLogPosition());
        return new LogTailInformation(
                loadConsensusIndexIfNeeded(logFile, checkpoint),
                entries.isPresent(),
                entries.getTransactionId(),
                lowestLogVersion == UNKNOWN,
                highestLogVersion,
                entries.getEntryVersion(),
                checkpoint.storeId(),
                fallbackKernelVersionProvider);
    }

    private LogTailInformation noCheckpointLogTail(LogFile logFile, long highestLogVersion, long lowestLogVersion)
            throws IOException {
        var entries = getFirstTransactionId(logFile, lowestLogVersion);
        return new LogTailInformation(
                entries.isPresent(),
                entries.getTransactionId(),
                lowestLogVersion == UNKNOWN,
                highestLogVersion,
                entries.getEntryVersion(),
                fallbackKernelVersionProvider);
    }

    private StartCommitEntries getFirstTransactionId(LogFile logFile, long lowestLogVersion) throws IOException {
        LogPosition logPosition = LogPosition.UNSPECIFIED;
        if (logFile.versionExists(lowestLogVersion)) {
            LogHeader logHeader = logFile.extractHeader(lowestLogVersion);
            if (logHeader != null) {
                logPosition = logHeader.getStartPosition();
            }
        }
        return getFirstTransactionIdAfterCheckpoint(logFile, logPosition);
    }

    /**
     * Valid checkpoint points to valid location in a file, which exists and header store id matches with checkpoint store id.
     * Otherwise, checkpoint is not considered valid, and we need to recover.
     */
    private boolean isValidCheckpoint(LogFile logFile, CheckpointInfo checkpointInfo) throws IOException {
        LogPosition logPosition = checkpointInfo.transactionLogPosition();
        long logVersion = logPosition.getLogVersion();
        if (!logFile.versionExists(logVersion)) {
            return false;
        }
        Path logFileForVersion = logFile.getLogFileForVersion(logVersion);
        if (fileSystem.getFileSize(logFileForVersion) < logPosition.getByteOffset()) {
            return false;
        }
        LogHeader logHeader = logFile.extractHeader(logVersion);
        if (logHeader == null) {
            return false;
        }
        StoreId headerStoreId = logHeader.getStoreId();
        return headerStoreId == null
                || headerStoreId.isSameOrUpgradeSuccessor(checkpointInfo.storeId())
                || checkpointInfo.storeId().isSameOrUpgradeSuccessor(headerStoreId);
    }

    private StartCommitEntries getFirstTransactionIdAfterCheckpoint(LogFile logFile, LogPosition logPosition)
            throws IOException {
        boolean corruptedTransactionLogs = false;
        LogEntryStart start = null;
        LogEntryCommit commit = null;
        LogEntryChunkEnd chunkEnd = null;
        LogPosition lookupPosition = null;
        if (logPosition != LogPosition.UNSPECIFIED) {
            long logVersion = logPosition.getLogVersion();
            try {
                while (logFile.versionExists(logVersion)) {
                    lookupPosition = lookupPosition == null
                            ? logPosition
                            : logFile.extractHeader(logVersion).getStartPosition();

                    var logEntryReader =
                            new VersionAwareLogEntryReader(commandReaderFactory, binarySupportedKernelVersions);
                    LogPosition position;
                    try (var reader = logFile.getReader(lookupPosition, NO_MORE_CHANNELS);
                            var cursor = new LogEntryCursor(logEntryReader, reader)) {
                        LogEntry entry;
                        while ((start == null || commit == null) && cursor.next()) {
                            entry = cursor.get();
                            if (commit == null && entry instanceof LogEntryCommit e) {
                                commit = e;
                            } else if (chunkEnd == null && entry instanceof LogEntryChunkEnd e) {
                                chunkEnd = e;
                            } else if (start == null && entry instanceof LogEntryStart e) {
                                start = e;
                            }
                        }
                        position = reader.getCurrentLogPosition();
                    }
                    if ((start != null) && (commit != null || chunkEnd != null)) {
                        return new StartCommitEntries(start, commit, chunkEnd);
                    }
                    // signal that we still need recovery since our logs look broken
                    corruptedTransactionLogs = logEntryReader.hasBrokenLastEntry();
                    // if the last tail record is partial we know that we will fail the next check
                    if (!corruptedTransactionLogs) {
                        verifyReaderPosition(logVersion, position);
                    }
                    logVersion++;
                }
            } catch (Error | ClosedByInterruptException e) {
                // These should not be parsing errors
                throw e;
            } catch (Throwable t) {
                monitor.corruptedLogFile(logVersion, t);
                if (failOnCorruptedLogFiles) {
                    throwUnableToCleanRecover(t);
                }
                corruptedTransactionLogs = true;
            }
        }
        return new StartCommitEntries(start, commit, chunkEnd, corruptedTransactionLogs);
    }

    private CheckpointInfo loadConsensusIndexIfNeeded(LogFile logFile, CheckpointInfo checkpoint) throws IOException {
        if (checkpoint.consensusIndexInCheckpoint()) {
            return checkpoint;
        }

        // this supports special case when upgrading from pre 5.7 to 5.10+
        // we can have 5.0 checkpoint without consensus index and need to parse it from transaction itself, if it is
        // available
        long requiredTransactionId = checkpoint.transactionId().id();
        long consensusIndex =
                findConsensusIndexForTransactionId(logFile, requiredTransactionId, checkpoint.transactionLogPosition());
        if (consensusIndex == UNKNOWN_CONSENSUS_INDEX) {
            // can't find consensus index from transaction logs
            return checkpoint;
        }
        // create new CheckpointInfo with new TransactionId which has proper consensus index
        return new CheckpointInfo(
                checkpoint.transactionLogPosition(),
                checkpoint.storeId(),
                checkpoint.checkpointEntryPosition(),
                checkpoint.channelPositionAfterCheckpoint(),
                checkpoint.checkpointFilePostReadPosition(),
                checkpoint.kernelVersion(),
                checkpoint.kernelVersionByte(),
                new TransactionId(
                        checkpoint.transactionId().id(),
                        checkpoint.transactionId().appendIndex(),
                        checkpoint.transactionId().kernelVersion(),
                        checkpoint.transactionId().checksum(),
                        checkpoint.transactionId().commitTimestamp(),
                        consensusIndex),
                checkpoint.appendIndex(),
                checkpoint.reason());
    }

    private long findConsensusIndexForTransactionId(
            LogFile logFile, long requiredTransactionId, LogPosition checkpointTransactionPosition) throws IOException {
        long logVersion = checkpointTransactionPosition.getLogVersion();
        var logEntryReader = new VersionAwareLogEntryReader(commandReaderFactory, binarySupportedKernelVersions);
        try {
            while (logFile.versionExists(logVersion)) {
                var logHeader = logFile.extractHeader(logVersion);
                if (logHeader != null) {
                    var logHeaderStart = logHeader.getStartPosition();
                    if (logHeaderStart.compareTo(checkpointTransactionPosition) < 0) {
                        try (var reader = logFile.getReader(logHeaderStart, NO_MORE_CHANNELS);
                                var cursor = new LogEntryCursor(logEntryReader, reader)) {
                            LogEntryStart start = null;

                            while (cursor.next()) {
                                LogEntry entry = cursor.get();
                                if (entry instanceof LogEntryStart e) {
                                    start = e;
                                } else if (entry instanceof LogEntryCommit commit) {
                                    if (start != null && commit.getTxId() == requiredTransactionId) {
                                        return LogIndexEncoding.decodeLogIndex(start.getAdditionalHeader());
                                    }
                                    start = null;
                                }
                            }
                        }
                    }
                }
                logVersion--;
            }
        } catch (Error | ClosedByInterruptException e) {
            // These should not be parsing errors
            throw e;
        } catch (Throwable t) {
            // ignore any parsing errors here, we only do our best to recover consensus index here
        }
        return UNKNOWN_CONSENSUS_INDEX;
    }

    private void verifyReaderPosition(long version, LogPosition logPosition) throws IOException {
        LogFile logFile = logFiles.getLogFile();
        long highestLogVersion = logFile.getHighestLogVersion();
        try (PhysicalLogVersionedStoreChannel channel = logFile.openForVersion(version)) {
            verifyLogChannel(channel, logPosition, version, highestLogVersion, true, TRANSACTION_LOG_NAME);
        }
    }

    private void verifyCheckpointPosition(LogPosition lastCheckpointPosition) throws IOException {
        long checkpointLogVersion = lastCheckpointPosition.getLogVersion();
        var checkpointFile = logFiles.getCheckpointFile();
        long highestLogVersion = checkpointFile.getHighestLogVersion();
        try (PhysicalLogVersionedStoreChannel channel = checkpointFile.openForVersion(checkpointLogVersion)) {
            channel.position(lastCheckpointPosition.getByteOffset());
            if (failOnCorruptedLogFiles) {
                verifyLogChannel(
                        channel,
                        lastCheckpointPosition,
                        checkpointLogVersion,
                        highestLogVersion,
                        false,
                        CHECKPOINT_LOG_NAME);
            }
        }
    }

    private void verifyLogChannel(
            PhysicalLogVersionedStoreChannel channel,
            LogPosition logPosition,
            long currentVersion,
            long highestVersion,
            boolean checkLastFile,
            String logName)
            throws IOException {
        verifyLogVersion(currentVersion, logPosition);
        long logFileSize = channel.size();
        long channelLeftovers = subtractExact(logFileSize, logPosition.getByteOffset());
        if (channelLeftovers != 0) {
            // channel has more data than entry reader can read. Only one valid case for this kind of situation is
            // pre-allocated log file that has some space left

            // if this log file is not the last one and we have some unreadable bytes in the end its an indication of
            // corrupted log files
            if (checkLastFile) {
                verifyLastFile(highestVersion, currentVersion, logPosition, logFileSize, channelLeftovers, logName);
            }

            // to double check that even when we encountered end of records position we do not have anything after that
            // we will try to read some data (up to 12K) in advance to check that only zero's are available there
            verifyNoMoreReadableDataAvailable(currentVersion, channel, logPosition, channelLeftovers, logName);
        }
    }

    private void verifyLogVersion(long version, LogPosition logPosition) {
        if (logPosition.getLogVersion() != version) {
            throw new IllegalStateException(format(
                    "Expected to observe log positions only for log file with version %d but encountered "
                            + "version %d while reading %s.",
                    version,
                    logPosition.getLogVersion(),
                    getCanonicalFile(logFiles.getLogFile().getLogFileForVersion(version))));
        }
    }

    static void throwUnableToCleanRecover(Throwable t) {
        throw new RuntimeException(
                "Error reading transaction logs, recovery not possible. To force the database to start anyway, you can specify '"
                        + GraphDatabaseInternalSettings.fail_on_corrupted_log_files.name()
                        + "=false'. This will try to recover as much "
                        + "as possible and then truncate the corrupt part of the transaction log. Doing this means your database "
                        + "integrity might be compromised, please consider restoring from a consistent backup instead.",
                t);
    }

    private static void verifyLastFile(
            long highestLogVersion,
            long version,
            LogPosition logPosition,
            long logFileSize,
            long channelLeftovers,
            String logName) {
        if (version != highestLogVersion) {
            throw new RuntimeException(format(
                    "%s log files with version %d has %d unreadable bytes. Was able to read upto %d but %d is available.",
                    logName, version, channelLeftovers, logPosition.getByteOffset(), logFileSize));
        }
    }

    private void verifyNoMoreReadableDataAvailable(
            long version,
            LogVersionedStoreChannel channel,
            LogPosition logPosition,
            long channelLeftovers,
            String logName)
            throws IOException {
        long initialPosition = channel.position();
        try {
            channel.position(logPosition.getByteOffset());
            try (var scopedBuffer = new HeapScopedBuffer(
                    safeCastLongToInt(min(kibiBytes(12), channelLeftovers)), ByteOrder.LITTLE_ENDIAN, memoryTracker)) {
                ByteBuffer byteBuffer = scopedBuffer.getBuffer();
                channel.readAll(byteBuffer);
                byteBuffer.flip();
                if (!isAllZerosBuffer(byteBuffer)) {
                    throw new RuntimeException(format(
                            "%s log file with version %d has some data available after last readable log entry. "
                                    + "Last readable position %d, read ahead buffer content: %s.",
                            logName, version, logPosition.getByteOffset(), dumpBufferToString(byteBuffer)));
                }
            }
        } finally {
            channel.position(initialPosition);
        }
    }

    /**
     * Collects information about the tail of the transaction log, i.e. last checkpoint, last entry etc.
     * Since this is an expensive task we do it once and reuse the result. This method is thus lazy and the first one
     * calling it will take the hit.
     * <p>
     * This is only intended to be used during startup. If you need to track the state of the tail, that can be done more
     * efficiently at runtime, and this method should then only be used to restore said state.
     *
     * @return snapshot of the state of the transaction logs tail at startup.
     */
    public LogTailMetadata getTailMetadata() {
        if (logTail == null) {
            logTail = findLogTail();
        }

        return logTail;
    }

    private static String dumpBufferToString(ByteBuffer byteBuffer) {
        byte[] data = new byte[byteBuffer.limit()];
        byteBuffer.get(data);
        return Arrays.toString(data);
    }

    private static boolean isAllZerosBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer.hasArray()) {
            byte[] array = byteBuffer.array();
            for (byte b : array) {
                if (b != 0) {
                    return false;
                }
            }
        } else {
            while (byteBuffer.hasRemaining()) {
                if (byteBuffer.get() != 0) {
                    return false;
                }
            }
        }
        return true;
    }

    private static class StartCommitEntries {
        private final LogEntryStart start;
        private final LogEntryCommit commit;
        private final LogEntryChunkEnd chunkEnd;
        private final boolean corruptedLogs;

        StartCommitEntries(LogEntryStart start, LogEntryCommit commit, LogEntryChunkEnd chunkEnd) {
            this(start, commit, chunkEnd, false);
        }

        StartCommitEntries(
                LogEntryStart start, LogEntryCommit commit, LogEntryChunkEnd chunkEnd, boolean corruptedLogs) {
            this.start = start;
            this.commit = commit;
            this.chunkEnd = chunkEnd;
            this.corruptedLogs = corruptedLogs;
        }

        public long getTransactionId() {
            if (commit != null) {
                return commit.getTxId();
            }
            if (chunkEnd != null) {
                return chunkEnd.getTransactionId();
            }
            return NO_TRANSACTION_ID;
        }

        public boolean isPresent() {
            return start != null || corruptedLogs;
        }

        public byte getEntryVersion() {
            if (start != null) {
                return start.kernelVersion().version();
            }
            return NO_ENTRY;
        }
    }
}
