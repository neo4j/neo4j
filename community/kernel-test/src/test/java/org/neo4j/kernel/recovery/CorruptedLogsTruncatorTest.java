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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.transaction.log.entry.v57.DetachedCheckpointLogEntrySerializerV5_7.RECORD_LENGTH_BYTES;
import static org.neo4j.kernel.recovery.CorruptedLogsTruncator.CORRUPTED_TX_LOGS_BASE_NAME;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION_PROVIDER;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.FlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class CorruptedLogsTruncatorTest {
    private static final long SINGLE_LOG_FILE_SIZE = LATEST_LOG_FORMAT.getHeaderSize() + 9L;
    private static final int TOTAL_NUMBER_OF_TRANSACTION_LOG_FILES = 12;
    // There is one file for the separate checkpoints as well
    private static final int TOTAL_NUMBER_OF_LOG_FILES = 13;
    private static final long ROTATION_THRESHOLD = 1024L;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory testDirectory;

    private final LifeSupport life = new LifeSupport();

    private Path databaseDirectory;
    private LogFiles logFiles;
    private CorruptedLogsTruncator logPruner;
    private SimpleLogVersionRepository logVersionRepository;
    private SimpleTransactionIdStore transactionIdStore;

    @BeforeEach
    void setUp() throws Exception {
        databaseDirectory = testDirectory.homePath();
        logVersionRepository = new SimpleLogVersionRepository();
        transactionIdStore = new SimpleTransactionIdStore();
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder(databaseDirectory, fs)
                .withRotationThreshold(SINGLE_LOG_FILE_SIZE)
                .withKernelVersionProvider(LATEST_KERNEL_VERSION_PROVIDER)
                .withLogVersionRepository(logVersionRepository)
                .withTransactionIdStore(transactionIdStore)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .withConfig(Config.newBuilder()
                        .set(
                                GraphDatabaseInternalSettings.checkpoint_logical_log_rotation_threshold,
                                ROTATION_THRESHOLD)
                        .build())
                .build();
        life.add(logFiles);
        logPruner = new CorruptedLogsTruncator(databaseDirectory, logFiles, fs, INSTANCE);
    }

    @AfterEach
    void tearDown() {
        life.shutdown();
    }

    @Test
    void doNotPruneEmptyLogs() throws IOException {
        logPruner.truncate(new LogPosition(0, LATEST_LOG_FORMAT.getHeaderSize()));
        assertTrue(FileSystemUtils.isEmptyOrNonExistingDirectory(fs, databaseDirectory));
    }

    @Test
    void doNotPruneNonCorruptedLogs() throws IOException {
        life.start();
        generateTransactionLogFiles(logFiles);

        var logFile = logFiles.getLogFile();
        long highestLogVersion = logFile.getHighestLogVersion();
        long fileSizeBeforePrune = Files.size(logFile.getHighestLogFile());
        LogPosition endOfLogsPosition = new LogPosition(highestLogVersion, fileSizeBeforePrune);
        assertEquals(TOTAL_NUMBER_OF_TRANSACTION_LOG_FILES - 1, highestLogVersion);

        logPruner.truncate(endOfLogsPosition);

        assertEquals(TOTAL_NUMBER_OF_LOG_FILES, logFiles.logFiles().length);
        assertEquals(fileSizeBeforePrune, Files.size(logFile.getHighestLogFile()));
        assertTrue(ArrayUtils.isEmpty(databaseDirectory.toFile().listFiles(File::isDirectory)));
    }

    @Test
    void doNotTruncateLogWithPreAllocatedZeros() throws IOException {
        life.start();
        generateTransactionLogFiles(logFiles);

        var logFile = logFiles.getLogFile();
        long highestLogVersion = logFile.getHighestLogVersion();
        long fileSizeBeforeAppend = Files.size(logFile.getHighestLogFile());
        LogPosition endOfLogsPosition = new LogPosition(highestLogVersion, fileSizeBeforeAppend);

        FlushableLogPositionAwareChannel channel =
                logFile.getTransactionLogWriter().getChannel();
        for (int i = 0; i < RandomUtils.nextInt(100, 10240); i++) {
            channel.putLong(0);
        }
        channel.prepareForFlush().flush();

        long fileAfterZeroAppend = Files.size(logFile.getHighestLogFile());

        assertNotEquals(fileSizeBeforeAppend, fileAfterZeroAppend);

        logPruner.truncate(endOfLogsPosition);

        assertEquals(TOTAL_NUMBER_OF_LOG_FILES, logFiles.logFiles().length);
        assertEquals(fileAfterZeroAppend, Files.size(logFile.getHighestLogFile()));
        assertNotEquals(fileSizeBeforeAppend, Files.size(logFile.getHighestLogFile()));
        assertTrue(ArrayUtils.isEmpty(databaseDirectory.toFile().listFiles(File::isDirectory)));
    }

    @Test
    void truncateLogWithCorruptionThatLooksLikePreAllocatedZeros() throws IOException {
        life.start();
        generateTransactionLogFiles(logFiles);

        var logFile = logFiles.getLogFile();
        long highestLogVersion = logFile.getHighestLogVersion();
        long fileSizeBeforeAppend = Files.size(logFile.getHighestLogFile());
        LogPosition endOfLogsPosition = new LogPosition(highestLogVersion, fileSizeBeforeAppend);

        FlushableLogPositionAwareChannel channel =
                logFile.getTransactionLogWriter().getChannel();
        for (int i = 0; i < RandomUtils.nextInt(100, 10240); i++) {
            channel.putLong(0);
        }
        // corruption byte
        channel.put((byte) 7);
        for (int i = 0; i < RandomUtils.nextInt(10, 1024); i++) {
            channel.putLong(0);
        }
        channel.prepareForFlush().flush();

        long fileAfterZeroAppend = Files.size(logFile.getHighestLogFile());
        assertNotEquals(fileSizeBeforeAppend, fileAfterZeroAppend);

        logPruner.truncate(endOfLogsPosition);

        assertEquals(TOTAL_NUMBER_OF_LOG_FILES, logFiles.logFiles().length);
        assertEquals(fileSizeBeforeAppend, Files.size(logFile.getHighestLogFile()));

        Path corruptedLogsDirectory = databaseDirectory.resolve(CORRUPTED_TX_LOGS_BASE_NAME);
        assertTrue(Files.exists(corruptedLogsDirectory));
        File[] files = corruptedLogsDirectory.toFile().listFiles();
        assertNotNull(files);
        assertEquals(1, files.length);
    }

    @Test
    void pruneAndArchiveLastLog() throws IOException {
        life.start();
        generateTransactionLogFiles(logFiles);

        var logFile = logFiles.getLogFile();
        long highestLogVersion = logFile.getHighestLogVersion();
        Path highestLogFile = logFile.getHighestLogFile();
        long fileSizeBeforePrune = Files.size(highestLogFile);
        int bytesToPrune = 5;
        long byteOffset = fileSizeBeforePrune - bytesToPrune;
        LogPosition prunePosition = new LogPosition(highestLogVersion, byteOffset);

        logPruner.truncate(prunePosition);

        assertEquals(TOTAL_NUMBER_OF_LOG_FILES, logFiles.logFiles().length);
        assertEquals(byteOffset, Files.size(highestLogFile));

        Path corruptedLogsDirectory = databaseDirectory.resolve(CORRUPTED_TX_LOGS_BASE_NAME);
        assertTrue(Files.exists(corruptedLogsDirectory));
        File[] files = corruptedLogsDirectory.toFile().listFiles();
        assertNotNull(files);
        assertEquals(1, files.length);

        File corruptedLogsArchive = files[0];
        checkArchiveName(highestLogVersion, byteOffset, corruptedLogsArchive);
        try (ZipFile zipFile = new ZipFile(corruptedLogsArchive)) {
            assertEquals(1, zipFile.size());
            checkEntryNameAndSize(zipFile, highestLogFile.getFileName().toString(), bytesToPrune);
        }
    }

    @Test
    void pruneAndArchiveMultipleLogs() throws IOException {
        life.start();
        generateTransactionLogFiles(logFiles);

        long highestCorrectLogFileIndex = 5;
        var logFile = logFiles.getLogFile();
        Path highestCorrectLogFile = logFile.getLogFileForVersion(highestCorrectLogFileIndex);
        long fileSizeBeforePrune = Files.size(highestCorrectLogFile);
        long highestLogFileLength = Files.size(logFile.getHighestLogFile());
        int bytesToPrune = 7;
        long byteOffset = fileSizeBeforePrune - bytesToPrune;
        LogPosition prunePosition = new LogPosition(highestCorrectLogFileIndex, byteOffset);
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        TransactionId transactionId = transactionIdStore.getLastCommittedTransaction();
        checkpointFile
                .getCheckpointAppender()
                .checkPoint(
                        LogCheckPointEvent.NULL,
                        transactionId,
                        LatestVersions.LATEST_KERNEL_VERSION,
                        new LogPosition(highestCorrectLogFileIndex, byteOffset - 1),
                        Instant.now(),
                        "within okay transactions");
        /* Write checkpoints that should be truncated. Write enough to get them get them in two files. */
        for (int i = 0; i < 4; i++) {
            checkpointFile
                    .getCheckpointAppender()
                    .checkPoint(
                            LogCheckPointEvent.NULL,
                            transactionId,
                            LatestVersions.LATEST_KERNEL_VERSION,
                            new LogPosition(highestCorrectLogFileIndex, byteOffset + 1),
                            Instant.now(),
                            "in the part being truncated");
        }

        life.shutdown();

        logPruner.truncate(prunePosition);

        life.start();

        // 6 transaction log files and a checkpoint file
        logVersionRepository.setCheckpointLogVersion(0);
        assertEquals(7, logFiles.logFiles().length);
        assertEquals(byteOffset, Files.size(highestCorrectLogFile));
        assertThat(checkpointFile.getDetachedCheckpointFiles()).hasSize(1);
        assertEquals(
                LATEST_LOG_FORMAT.getHeaderSize() + RECORD_LENGTH_BYTES /* one checkpoint */,
                Files.size(checkpointFile.getDetachedCheckpointFiles()[0]));

        Path corruptedLogsDirectory = databaseDirectory.resolve(CORRUPTED_TX_LOGS_BASE_NAME);
        assertTrue(Files.exists(corruptedLogsDirectory));
        File[] files = corruptedLogsDirectory.toFile().listFiles();
        assertNotNull(files);
        assertEquals(1, files.length);

        File corruptedLogsArchive = files[0];
        checkArchiveName(highestCorrectLogFileIndex, byteOffset, corruptedLogsArchive);
        try (ZipFile zipFile = new ZipFile(corruptedLogsArchive)) {
            assertEquals(9, zipFile.size());
            checkEntryNameAndSize(zipFile, highestCorrectLogFile.getFileName().toString(), bytesToPrune);
            long nextLogFileIndex = highestCorrectLogFileIndex + 1;
            int lastFileIndex = TOTAL_NUMBER_OF_TRANSACTION_LOG_FILES - 1;
            for (long index = nextLogFileIndex; index < lastFileIndex; index++) {
                checkEntryNameAndSize(
                        zipFile, TransactionLogFilesHelper.DEFAULT_NAME + "." + index, SINGLE_LOG_FILE_SIZE);
            }
            checkEntryNameAndSize(
                    zipFile, TransactionLogFilesHelper.DEFAULT_NAME + "." + lastFileIndex, highestLogFileLength);
            checkEntryNameAndSize(
                    zipFile,
                    TransactionLogFilesHelper.CHECKPOINT_FILE_PREFIX + ".0",
                    RECORD_LENGTH_BYTES * 3 /* 3 checkpoints */);
            if (NativeAccessProvider.getNativeAccess().isAvailable()) {
                // whole file is corrupted in above scenario and its preallocated
                checkEntryNameAndSize(
                        zipFile, TransactionLogFilesHelper.CHECKPOINT_FILE_PREFIX + ".1", ROTATION_THRESHOLD);
            } else {
                // whole file is corrupted in above scenario and file does not have any empty space after last available
                // data point
                checkEntryNameAndSize(
                        zipFile,
                        TransactionLogFilesHelper.CHECKPOINT_FILE_PREFIX + ".1",
                        LATEST_LOG_FORMAT.getHeaderSize() + RECORD_LENGTH_BYTES /* one checkpoint */);
            }
        }
    }

    private static void checkEntryNameAndSize(ZipFile zipFile, String entryName, long expectedSize) throws IOException {
        ZipEntry entry = zipFile.getEntry(entryName);
        InputStream inputStream = zipFile.getInputStream(entry);
        int entryBytes = 0;
        while (inputStream.read() >= 0) {
            entryBytes++;
        }
        assertEquals(expectedSize, entryBytes);
    }

    private static void checkArchiveName(long highestLogVersion, long byteOffset, File corruptedLogsArchive) {
        String name = corruptedLogsArchive.getName();
        assertTrue(name.startsWith("corrupted-neostore.transaction.db-" + highestLogVersion + "-" + byteOffset));
        assertTrue(FilenameUtils.isExtension(name, "zip"));
    }

    private static void generateTransactionLogFiles(LogFiles logFiles) throws IOException {
        LogFile logFile = logFiles.getLogFile();
        FlushableLogPositionAwareChannel writer =
                logFile.getTransactionLogWriter().getChannel();
        for (byte i = 0; i < 107; i++) {
            writer.put(i);
            writer.prepareForFlush();
            if (logFile.rotationNeeded()) {
                logFile.rotate();
            }
        }
    }
}
