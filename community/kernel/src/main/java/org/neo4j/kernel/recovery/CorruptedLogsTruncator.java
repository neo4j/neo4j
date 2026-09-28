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
package org.neo4j.kernel.recovery;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.neo4j.internal.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.io.ByteUnit.MebiByte;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.IOUtils.uncheckedLongConsumer;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.wal.CheckpointInfo;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.PhysicalLogVersionedStoreChannel;
import org.neo4j.wal.WALService;
import org.neo4j.wal.checkpoint.CheckpointFile;
import org.neo4j.wal.entry.LogHeaderReader;
import org.neo4j.wal.files.LogRangeInfo;
import org.neo4j.wal.files.TransactionLogFilesHelper;

/**
 * Transaction log truncator used during recovery to truncate all the logs after some specified position, that
 * recovery treats as corrupted or non-readable.
 * Transaction log file specified by provided log position will be truncated to provided length, any
 * subsequent files will be removed.
 * Any removed or modified log content will be stored in separate corruption logs archive for further analysis and as
 * an additional safety option to have the possibility to fully restore original logs in a faulty case.
 */
public class CorruptedLogsTruncator {
    public static final String CORRUPTED_TX_LOGS_BASE_NAME = "corrupted-" + TransactionLogFilesHelper.DEFAULT_NAME;
    private static final String LOG_FILE_ARCHIVE_PATTERN = CORRUPTED_TX_LOGS_BASE_NAME + "-%d-%d-%d.zip";

    private final Path storeDir;
    private final WALService walService;
    private final FileSystemAbstraction fs;
    private final MemoryTracker memoryTracker;

    public CorruptedLogsTruncator(
            Path storeDir, WALService walService, FileSystemAbstraction fs, MemoryTracker memoryTracker) {
        this.storeDir = storeDir;
        this.walService = walService;
        this.fs = fs;
        this.memoryTracker = memoryTracker;
    }

    /**
     * Truncate all transaction logs after provided position. Log version specified in a position will be
     * truncated to provided byte offset, any subsequent log files will be deleted.
     * Any checkpoints pointing ahead of the checkpoint will be removed.
     * If null is passed as lastCheckpoint then all checkpoints are cleared
     * Backup copy of removed data will be stored in separate archive.
     *
     * @param positionAfterLastRecoveredTransaction position after last recovered transaction
     * @param lastCheckpoint                        last known checkpoint
     * @throws IOException
     */
    public void truncate(LogPosition positionAfterLastRecoveredTransaction, CheckpointInfo lastCheckpoint)
            throws IOException {
        long recoveredTransactionLogVersion = positionAfterLastRecoveredTransaction.getLogVersion();
        long recoveredTransactionOffset = positionAfterLastRecoveredTransaction.getByteOffset();
        boolean txLogCorrupted = isRecoveredLogCorrupted(recoveredTransactionLogVersion, recoveredTransactionOffset)
                || haveMoreRecentLogFiles(recoveredTransactionLogVersion);

        CheckpointFileInfo checkpointFileInfo = checkCheckpointFileNeedsTruncation(lastCheckpoint);
        if (txLogCorrupted || checkpointFileInfo.needTruncation) {
            backupCorruptedContent(recoveredTransactionLogVersion, recoveredTransactionOffset, checkpointFileInfo);
            truncateLogFiles(recoveredTransactionLogVersion, recoveredTransactionOffset, checkpointFileInfo);
        }
    }

    private void truncateLogFiles(
            long recoveredTransactionLogVersion, long recoveredTransactionOffset, CheckpointFileInfo checkpointFileInfo)
            throws IOException {
        LogFile transactionLogFile = walService.getLogFile();
        truncateFilesFromVersion(
                recoveredTransactionLogVersion,
                recoveredTransactionOffset,
                transactionLogFile.getLogRangeInfo().highestVersion(),
                transactionLogFile::getLogFileForVersion);

        if (checkpointFileInfo.needTruncation) {
            LogPosition checkpointPosition = checkpointFileInfo.place;
            CheckpointFile checkpointFile = walService.getCheckpointFile();

            truncateFilesFromVersion(
                    checkpointPosition.getLogVersion(),
                    checkpointPosition.getByteOffset(),
                    checkpointFile.getCurrentLogVersion(),
                    checkpointFile::getLogFileForVersion);
        }
    }

    public void forceTruncateLogFiles(LogPosition txLogPosition, LogPosition checkpointLogPosition) throws IOException {
        truncateLogFiles(
                txLogPosition.getLogVersion(),
                txLogPosition.getByteOffset(),
                new CheckpointFileInfo(checkpointLogPosition != null, checkpointLogPosition));
    }

    private void truncateFilesFromVersion(
            long recoveredLogVersion,
            long recoveredOffset,
            long highestLogVersion,
            Function<Long, Path> getFileForVersion)
            throws IOException {
        Path lastRecoveredLog = getFileForVersion.apply(recoveredLogVersion);
        fs.truncate(lastRecoveredLog, recoveredOffset);
        forEachSubsequentFile(
                recoveredLogVersion,
                highestLogVersion,
                uncheckedLongConsumer(fileIndex -> fs.deleteFile(getFileForVersion.apply(fileIndex))));
    }

    private static void forEachSubsequentFile(long recoveredLogVersion, long highestLogVersion, LongConsumer action) {
        for (long fileIndex = recoveredLogVersion + 1; fileIndex <= highestLogVersion; fileIndex++) {
            action.accept(fileIndex);
        }
    }

    private void backupCorruptedContent(
            long recoveredTransactionLogVersion, long recoveredTransactionOffset, CheckpointFileInfo checkpointFileInfo)
            throws IOException {
        Path corruptedLogArchive = getArchiveFile(recoveredTransactionLogVersion, recoveredTransactionOffset);
        try (ZipOutputStream recoveryContent = new ZipOutputStream(fs.openAsOutputStream(corruptedLogArchive, false));
                var bufferScope =
                        new HeapScopedBuffer(toIntExact(MebiByte.toBytes(1)), ByteOrder.LITTLE_ENDIAN, memoryTracker)) {
            LogFile transactionLogFile = walService.getLogFile();
            copyLogsContent(
                    recoveredTransactionLogVersion,
                    recoveredTransactionOffset,
                    transactionLogFile.getLogRangeInfo().highestVersion(),
                    recoveryContent,
                    bufferScope,
                    transactionLogFile::getLogFileForVersion);

            if (checkpointFileInfo.needTruncation) {
                LogPosition checkpointPosition = checkpointFileInfo.place;
                CheckpointFile checkpointFile = walService.getCheckpointFile();

                copyLogsContent(
                        checkpointPosition.getLogVersion(),
                        checkpointPosition.getByteOffset(),
                        checkpointFile.getCurrentLogVersion(),
                        recoveryContent,
                        bufferScope,
                        checkpointFile::getLogFileForVersion);
            }
        }
    }

    private void copyLogsContent(
            long recoveredLogVersion,
            long recoveredOffset,
            long highestLogVersion,
            ZipOutputStream recoveryContent,
            HeapScopedBuffer bufferScope,
            Function<Long, Path> getFileForVersion)
            throws IOException {
        copyLogContent(
                recoveredLogVersion, recoveredOffset, recoveryContent, bufferScope.getBuffer(), getFileForVersion);
        forEachSubsequentFile(recoveredLogVersion, highestLogVersion, fileIndex -> {
            try {
                copyLogContent(fileIndex, 0, recoveryContent, bufferScope.getBuffer(), getFileForVersion);
            } catch (IOException io) {
                throw new UncheckedIOException(io);
            }
        });
    }

    private Path getArchiveFile(long recoveredTransactionLogVersion, long recoveredTransactionOffset)
            throws IOException {
        Path corruptedLogsFolder = storeDir.resolve(CORRUPTED_TX_LOGS_BASE_NAME);
        fs.mkdirs(corruptedLogsFolder);
        return corruptedLogsFolder.resolve(format(
                LOG_FILE_ARCHIVE_PATTERN,
                recoveredTransactionLogVersion,
                recoveredTransactionOffset,
                System.currentTimeMillis()));
    }

    private void copyLogContent(
            long logFileIndex,
            long logOffset,
            ZipOutputStream destination,
            ByteBuffer byteBuffer,
            Function<Long, Path> getFileForVersion)
            throws IOException {
        Path logFile = getFileForVersion.apply(logFileIndex);
        if (fs.getFileSize(logFile) == logOffset) {
            // file was recovered fully, nothing to backup
            return;
        }
        addLogFileToZipStream(logOffset, destination, byteBuffer, logFile);
    }

    private void addLogFileToZipStream(long logOffset, ZipOutputStream destination, ByteBuffer byteBuffer, Path logFile)
            throws IOException {
        ZipEntry zipEntry = new ZipEntry(logFile.getFileName().toString());
        destination.putNextEntry(zipEntry);
        try (StoreChannel transactionLogChannel = fs.read(logFile)) {
            transactionLogChannel.position(logOffset);
            while (transactionLogChannel.read(byteBuffer) >= 0) {
                byteBuffer.flip();
                destination.write(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining());
                byteBuffer.clear();
            }
        }
        destination.closeEntry();
    }

    private boolean haveMoreRecentLogFiles(long recoveredTransactionLogVersion) {
        return walService.getLogFile().getLogRangeInfo().highestVersion() > recoveredTransactionLogVersion;
    }

    private boolean haveMoreRecentCheckpointLogFiles(long recoveredTransactionLogVersion) {
        return walService.getCheckpointFile().getLogRangeInfo().highestVersion() > recoveredTransactionLogVersion;
    }

    private boolean isRecoveredLogCorrupted(long recoveredTransactionLogVersion, long recoveredTransactionOffset)
            throws IOException {
        try {
            LogFile logFile = walService.getLogFile();
            if (fs.getFileSize(logFile.getLogFileForVersion(recoveredTransactionLogVersion))
                    > recoveredTransactionOffset) {
                return checkOnlyZeroesAfterPosition(
                        recoveredTransactionOffset, logFile.openForVersion(recoveredTransactionLogVersion));
            }
            return false;
        } catch (NoSuchFileException ignored) {
            return false;
        }
    }

    private boolean isRecoveredCheckpointLogCorrupted(
            long recoveredTransactionLogVersion, long recoveredTransactionOffset) throws IOException {
        try {
            CheckpointFile logFile = walService.getCheckpointFile();
            if (fs.getFileSize(logFile.getLogFileForVersion(recoveredTransactionLogVersion))
                    > recoveredTransactionOffset) {
                return checkOnlyZeroesAfterPosition(
                        recoveredTransactionOffset, logFile.openForVersion(recoveredTransactionLogVersion));
            }
            return false;
        } catch (NoSuchFileException ignored) {
            return false;
        }
    }

    private boolean checkOnlyZeroesAfterPosition(
            long recoveredTransactionOffset, PhysicalLogVersionedStoreChannel physicalLogVersionedStoreChannel)
            throws IOException {
        try (PhysicalLogVersionedStoreChannel channel = physicalLogVersionedStoreChannel;
                var scopedBuffer = new NativeScopedBuffer(
                        safeCastLongToInt(kibiBytes(64)), ByteOrder.LITTLE_ENDIAN, memoryTracker)) {
            channel.position(recoveredTransactionOffset);
            ByteBuffer byteBuffer = scopedBuffer.getBuffer();
            while (channel.read(byteBuffer) >= 0) {
                byteBuffer.flip();
                if (ByteBuffers.directBufferContainsNonZeroData(byteBuffer)) {
                    return true;
                }
                byteBuffer.clear();
            }
        }
        return false;
    }

    private CheckpointFileInfo checkCheckpointFileNeedsTruncation(CheckpointInfo lastCheckpoint) throws IOException {
        if (lastCheckpoint != null
                && (!lastCheckpoint
                                .channelPositionAfterCheckpoint()
                                .equals(lastCheckpoint.checkpointFilePostReadPosition())
                        || isRecoveredCheckpointLogCorrupted(
                                lastCheckpoint.channelPositionAfterCheckpoint().getLogVersion(),
                                lastCheckpoint.channelPositionAfterCheckpoint().getByteOffset())
                        || haveMoreRecentCheckpointLogFiles(
                                lastCheckpoint.channelPositionAfterCheckpoint().getLogVersion()))) {
            return new CheckpointFileInfo(true, lastCheckpoint.channelPositionAfterCheckpoint());
        }
        // We didn't see any checkpoints, but that doesn't mean there isn't unreadable things in the log.
        // Let's do a best effort to see if there is any garbage in the file to remove.
        if (lastCheckpoint == null && walService.getCheckpointFile().getCurrentLogVersion() >= INITIAL_LOG_VERSION) {
            try {
                LogRangeInfo logRangeInfo = walService.getCheckpointFile().getLogRangeInfo();
                LogPosition startPosition = LogHeaderReader.readLogHeader(
                                fs, logRangeInfo.lowestFile(), EmptyMemoryTracker.INSTANCE)
                        .getStartPosition();
                if (isRecoveredCheckpointLogCorrupted(startPosition.getLogVersion(), startPosition.getByteOffset())) {
                    return new CheckpointFileInfo(true, startPosition);
                }
            } catch (NoSuchFileException ignore) {
            } catch (Exception e) {
                throw new RuntimeException("Failed to handle corrupt checkpoint file", e);
            }
        }
        return new CheckpointFileInfo(false, LogPosition.UNSPECIFIED);
    }

    record CheckpointFileInfo(boolean needTruncation, LogPosition place) {}
}
