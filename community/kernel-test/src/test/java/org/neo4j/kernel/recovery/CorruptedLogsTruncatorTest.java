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
import static org.neo4j.kernel.impl.transaction.log.entry.v202608.DetachedCheckpointLogEntrySerializerV2026_08.RECORD_LENGTH_BYTES;
import static org.neo4j.kernel.recovery.CorruptedLogsTruncator.CORRUPTED_TX_LOGS_BASE_NAME;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION_PROVIDER;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT_PROVIDER;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Arrays;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.FlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.LogRangeInfo;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@RandomSupportExtension
class CorruptedLogsTruncatorTest {
    private static final int TOTAL_NUMBER_OF_TRANSACTION_LOG_FILES = 12;
    // There is one file for the separate checkpoints as well
    private static final int TOTAL_NUMBER_OF_LOG_FILES = 13;
    private static final int ROTATION_THRESHOLD = 1024;
    private static final int PAYLOAD_LENGTH = ROTATION_THRESHOLD / 2;
    private static final int SEGMENT_SIZE = ROTATION_THRESHOLD / 4;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private RandomSupport random;

    private final LifeSupport life = new LifeSupport();

    private Path databaseDirectory;
    private LogFiles logFiles;
    private CorruptedLogsTruncator logPruner;
    private SimpleLogVersionRepository logVersionRepository;

    @BeforeEach
    void setUp() throws Exception {
        databaseDirectory = testDirectory.homePath();
        logVersionRepository = new SimpleLogVersionRepository();
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        logFiles = LogFilesBuilder.writeableBuilder(
                        DatabaseLayout.ofFlat(databaseDirectory),
                        fs,
                        LATEST_KERNEL_VERSION_PROVIDER,
                        LATEST_LOG_FORMAT_PROVIDER)
                .withBufferSizeBytes(ROTATION_THRESHOLD)
                .withRotationThreshold(ROTATION_THRESHOLD)
                .withEnvelopeSegmentBlockSizeBytes(SEGMENT_SIZE)
                .withKernelVersionProvider(LATEST_KERNEL_VERSION_PROVIDER)
                .withLogFormatVersionProvider(LATEST_LOG_FORMAT_PROVIDER)
                .withLogVersionRepository(logVersionRepository)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .withConfig(Config.newBuilder()
                        .set(GraphDatabaseInternalSettings.checkpoint_logical_log_rotation_threshold, (long)
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
        logPruner.truncate(new LogPosition(0, LATEST_LOG_FORMAT.getHeaderSize()), null);
        assertTrue(FileSystemUtils.isEmptyOrNonExistingDirectory(fs, databaseDirectory));
    }

    @Test
    void doNotPruneNonCorruptedLogs() throws IOException {
        life.start();
        LogPosition logPosAfterGeneratingLogs = generateTransactionLogFiles(logFiles);

        var logFile = logFiles.getLogFile();
        LogRangeInfo logRangeInfo = logFile.getLogRangeInfo();
        long highestLogVersion = logRangeInfo.highestVersion();
        long expectedFileSizeAfterTruncate = Files.size(logRangeInfo.highestFile());
        assertEquals(TOTAL_NUMBER_OF_TRANSACTION_LOG_FILES - 1, highestLogVersion);

        logPruner.truncate(logPosAfterGeneratingLogs, null);

        assertEquals(TOTAL_NUMBER_OF_LOG_FILES, logFiles.logFiles().length);
        assertEquals(
                expectedFileSizeAfterTruncate,
                Files.size(logFile.getLogRangeInfo().highestFile()));
        assertTrue(ArrayUtils.isEmpty(databaseDirectory.toFile().listFiles(File::isDirectory)));
    }

    @Test
    void doNotTruncateLogWithPreAllocatedZeros() throws IOException {
        life.start();
        LogPosition logPosAfterGeneratingLogs = generateTransactionLogFiles(logFiles);

        var logFile = logFiles.getLogFile();
        try (StoreChannel raw = fs.open(
                logFile.getLogFileForVersion(logPosAfterGeneratingLogs.getLogVersion()),
                Set.of(StandardOpenOption.APPEND, StandardOpenOption.WRITE))) {
            int zeroes = random.nextInt(100, 10240);
            raw.write(ByteBuffer.wrap(new byte[zeroes]));
            assertNotEquals(logPosAfterGeneratingLogs.getByteOffset(), raw.position());
        }

        long expectedFileSizeAfterTruncate =
                Files.size(logFile.getLogRangeInfo().highestFile());
        logPruner.truncate(logPosAfterGeneratingLogs, null);

        assertEquals(TOTAL_NUMBER_OF_LOG_FILES, logFiles.logFiles().length);
        assertEquals(
                expectedFileSizeAfterTruncate,
                Files.size(logFile.getLogRangeInfo().highestFile()));
        assertTrue(ArrayUtils.isEmpty(databaseDirectory.toFile().listFiles(File::isDirectory)));
    }

    @Test
    void truncateLogWithCorruptionThatLooksLikePreAllocatedZeros() throws IOException {
        life.start();
        LogPosition logPosAfterGeneratingLogs = generateTransactionLogFiles(logFiles);
        long expectedFileSizeAfterTruncate = logPosAfterGeneratingLogs.getByteOffset();

        var logFile = logFiles.getLogFile();
        FlushableLogPositionAwareChannel channel =
                logFile.getTransactionLogWriter().getChannel();
        // Pad with zeroes before the corrupted byte
        int beforeZeroes = random.nextInt(100, 10240);
        channel.putVersion(LATEST_KERNEL_VERSION.version());
        channel.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        channel.put(new byte[beforeZeroes], beforeZeroes);
        // corruption byte
        channel.put((byte) 7);
        // After corrupted byte, pad with a few more zeroes.
        int afterZeroes = random.nextInt(10, 1024);
        channel.put(new byte[afterZeroes], afterZeroes);
        channel.putChecksum();
        channel.prepareForFlush().flush();
        assertNotEquals(logPosAfterGeneratingLogs, channel.getCurrentLogPosition());

        logPruner.truncate(logPosAfterGeneratingLogs, null);

        assertEquals(TOTAL_NUMBER_OF_LOG_FILES, logFiles.logFiles().length);
        assertEquals(
                expectedFileSizeAfterTruncate,
                Files.size(logFile.getLogRangeInfo().highestFile()));

        Path corruptedLogsDirectory = databaseDirectory.resolve(CORRUPTED_TX_LOGS_BASE_NAME);
        assertTrue(Files.exists(corruptedLogsDirectory));
        File[] files = corruptedLogsDirectory.toFile().listFiles();
        assertNotNull(files);
        assertEquals(1, files.length);
    }

    @Test
    @EnabledOnOs(OS.LINUX) // based on pre-allocated files, which does not work on windows
    void pruneAndArchiveLastLog() throws IOException {
        life.start();
        LogPosition logPosAfterGeneratingLogs = generateTransactionLogFiles(logFiles);

        var logFile = logFiles.getLogFile();
        LogRangeInfo logRangeInfo = logFile.getLogRangeInfo();
        long highestLogVersion = logRangeInfo.highestVersion();
        Path highestLogFile = logRangeInfo.highestFile();
        int bytesToPrune = 5; // 1 byte for (byte)42 + 4 bytes for the checksum, see generateTransactionLogFiles().
        long byteOffset = logPosAfterGeneratingLogs.getByteOffset() - bytesToPrune;
        LogPosition prunePosition = new LogPosition(highestLogVersion, byteOffset);

        logPruner.truncate(prunePosition, null);

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
            checkEntryNameAndSize(zipFile, highestLogFile.getFileName().toString(), ROTATION_THRESHOLD - byteOffset);
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
        long highestLogFileLength = Files.size(logFile.getLogRangeInfo().highestFile());
        int bytesToPrune = 7;
        long byteOffset = fileSizeBeforePrune - bytesToPrune;
        LogPosition prunePosition = new LogPosition(highestCorrectLogFileIndex, byteOffset);
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        TransactionId transactionId = logFiles.logMetadataProvider().getLastCommittedTransaction();
        // Write checkpoints that should be truncated. Write enough to get them in two files.
        int checkpointsToTruncate = 0;
        while (checkpointFile.getCurrentLogVersion() == 0L) {
            checkpointFile
                    .getCheckpointAppender()
                    .checkPoint(
                            LogCheckPointEvent.NULL,
                            transactionId,
                            transactionId.id() + 7,
                            LATEST_KERNEL_VERSION,
                            new LogPosition(highestCorrectLogFileIndex, byteOffset + 1),
                            new LogPosition(highestCorrectLogFileIndex, byteOffset + 1),
                            Instant.now(),
                            "in the part being truncated");
            ++checkpointsToTruncate;
        }
        long secondCheckpointFileSize = Files.size(checkpointFile.getCurrentFile());

        life.shutdown();

        logPruner.truncate(prunePosition, null);

        life.start();

        // 6 transaction log files and a checkpoint file
        logVersionRepository.setCheckpointLogVersion(0);
        assertEquals(7, logFiles.logFiles().length);
        assertEquals(byteOffset, Files.size(highestCorrectLogFile));
        assertThat(checkpointFile.getMatchedFiles()).hasSize(1);
        // Truncate assumes that all checkpoints are broken when null is sent in as last checkpoint
        long expectedEmptyFileSize =
                LATEST_LOG_FORMAT.usesSegments() ? SEGMENT_SIZE : LATEST_LOG_FORMAT.getHeaderSize();
        assertEquals(expectedEmptyFileSize, Files.size(checkpointFile.getMatchedFiles()[0]));

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
            // For non-enveloped written tx size includes version and checksum
            long txSize = PAYLOAD_LENGTH + Byte.BYTES + Integer.BYTES;
            long expectedFullLogFileSize = LATEST_LOG_FORMAT.usesSegments()
                    // Enveloped files are rolled on the threshold and zero padded if required
                    ? ROTATION_THRESHOLD
                    // Non-Enveloped logs have header size plus just enough transaction to overflow rotation size
                    : LATEST_LOG_FORMAT.getHeaderSize() + (1 + ((ROTATION_THRESHOLD - 1) / txSize)) * txSize;
            for (long index = nextLogFileIndex; index < lastFileIndex; index++) {
                checkEntryNameAndSize(
                        zipFile, TransactionLogFilesHelper.DEFAULT_NAME + "." + index, expectedFullLogFileSize);
            }
            checkEntryNameAndSize(
                    zipFile, TransactionLogFilesHelper.DEFAULT_NAME + "." + lastFileIndex, highestLogFileLength);
            // first file in checkpoint archive only contains the removed content
            long expectedCorruptCheckpointSize = LATEST_LOG_FORMAT.usesSegments()
                    // Full size rotated file truncated back to header segment only
                    ? ROTATION_THRESHOLD - SEGMENT_SIZE
                    // Since PR25018 truncate with a null checkpoint deletes everything in the file
                    // i.e. all except the checkpoint that overflowed to new file
                    : (long) RECORD_LENGTH_BYTES * (checkpointsToTruncate - 1);
            checkEntryNameAndSize(
                    zipFile, TransactionLogFilesHelper.CHECKPOINT_FILE_PREFIX + ".0", expectedCorruptCheckpointSize);
            // Subsequent checkpoint file is placed entirely into zip
            checkEntryNameAndSize(
                    zipFile, TransactionLogFilesHelper.CHECKPOINT_FILE_PREFIX + ".1", secondCheckpointFileSize);
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

    /**
     * Generate transaction log files and returns the {@link LogPosition} for the last file written.
     */
    private static LogPosition generateTransactionLogFiles(LogFiles logFiles) throws IOException {
        byte[] payload = new byte[PAYLOAD_LENGTH];
        Arrays.fill(payload, (byte) 0xFF);

        LogFile logFile = logFiles.getLogFile();
        FlushableLogPositionAwareChannel writer =
                logFile.getTransactionLogWriter().getChannel();
        // Fill up all but the last log file
        while (logFile.getLogRangeInfo().highestVersion() < TOTAL_NUMBER_OF_TRANSACTION_LOG_FILES - 1) {
            writer.beginChecksumForWriting();
            writer.putVersion(LATEST_KERNEL_VERSION.version());
            writer.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);

            writer.put(payload, PAYLOAD_LENGTH);
            writer.putChecksum();
            if (logFile.rotationNeeded()) {
                logFile.rotate();
            }
        }
        // Write a small entry to the last log
        writer.beginChecksumForWriting();
        writer.putVersion(LATEST_KERNEL_VERSION.version());
        writer.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        writer.put((byte) 42);
        writer.putChecksum();
        writer.prepareForFlush().flush();

        return writer.getCurrentLogPosition();
    }
}
