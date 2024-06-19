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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.V6;
import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.CHECKPOINT_FILE_PREFIX;
import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.DEFAULT_NAME;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION_PROVIDER;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
class TransactionLogFilesTest {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void shouldGetTheFileNameForAGivenVersion() throws Exception {
        // given
        final LogFiles files = createLogFiles();
        final int version = 12;

        // when
        final Path versionFileName = files.getLogFile().getLogFileForVersion(version);

        // then
        final Path expected = createTransactionLogFile(databaseLayout, getVersionedLogFileName(version));
        assertEquals(expected, versionFileName);
    }

    @Test
    void extractHeaderOf3_5Format() throws Exception {
        LogFiles files = createLogFiles();

        create3_5FileWithHeader(databaseLayout, "0", 0);
        create3_5FileWithHeader(databaseLayout, "1", 1);
        create3_5FileWithHeader(databaseLayout, "2", 2);

        LogFile logFile = files.getLogFile();
        var logHeader = logFile.extractHeader(0);
        assertEquals(V6.getHeaderSize(), logHeader.getStartPosition().getByteOffset());
        assertEquals(V6, logHeader.getLogFormatVersion());
        assertEquals(
                V6.getHeaderSize(), logFile.extractHeader(1).getStartPosition().getByteOffset());
        assertEquals(
                V6.getHeaderSize(), logFile.extractHeader(2).getStartPosition().getByteOffset());
    }

    @Test
    void detectEntriesIn3_5Format() throws Exception {
        LogFiles files = createLogFiles();

        create3_5FileWithHeader(databaseLayout, "0", 0);
        create3_5FileWithHeader(databaseLayout, "1", 10);

        LogFile logFile = files.getLogFile();
        assertFalse(logFile.hasAnyEntries(0));
        assertTrue(logFile.hasAnyEntries(1));
    }

    @Test
    void shouldVisitEachLofFile() throws Throwable {
        // given
        LogFiles files = createLogFiles();

        fileSystem
                .write(createTransactionLogFile(databaseLayout, getVersionedLogFileName("1")))
                .close();
        fileSystem
                .write(createTransactionLogFile(databaseLayout, getVersionedLogFileName("some", "2")))
                .close();
        fileSystem
                .write(createTransactionLogFile(databaseLayout, getVersionedLogFileName("3")))
                .close();
        fileSystem.write(createTransactionLogFile(databaseLayout, DEFAULT_NAME)).close();

        // when
        final List<Path> seenFiles = new ArrayList<>();
        final List<Long> seenVersions = new ArrayList<>();

        files.getLogFile().accept((file, logVersion) -> {
            seenFiles.add(file);
            seenVersions.add(logVersion);
        });

        // then
        assertThat(seenFiles)
                .contains(
                        createTransactionLogFile(databaseLayout, getVersionedLogFileName(DEFAULT_NAME, "1")),
                        createTransactionLogFile(databaseLayout, getVersionedLogFileName(DEFAULT_NAME, "3")));
        assertThat(seenVersions).contains(1L, 3L);
        files.shutdown();
    }

    @Test
    void shouldBeAbleToRetrieveTheHighestLogVersion() throws Throwable {
        // given
        LogFiles files = createLogFiles();

        fileSystem
                .write(createTransactionLogFile(databaseLayout, getVersionedLogFileName("1")))
                .close();
        fileSystem
                .write(createTransactionLogFile(databaseLayout, getVersionedLogFileName("some", "4")))
                .close();
        fileSystem
                .write(createTransactionLogFile(databaseLayout, getVersionedLogFileName("3")))
                .close();
        fileSystem.write(createTransactionLogFile(databaseLayout, DEFAULT_NAME)).close();

        // when
        final long highestLogVersion = files.getLogFile().getHighestLogVersion();

        // then
        assertEquals(3, highestLogVersion);
        files.shutdown();
    }

    @Test
    void checkpointAndLogFilesAreIncludedInTheListOfFiles() throws Exception {
        LogFiles files = createLogFiles();

        fileSystem
                .write(createTransactionLogFile(databaseLayout, getVersionedLogFileName(1)))
                .close();
        fileSystem
                .write(createTransactionLogFile(databaseLayout, getVersionedLogFileName(2)))
                .close();
        fileSystem
                .write(createTransactionLogFile(databaseLayout, getVersionedCheckpointLogFileName(3)))
                .close();
        fileSystem
                .write(createTransactionLogFile(databaseLayout, getVersionedCheckpointLogFileName(4)))
                .close();

        var logFiles = files.logFiles();
        assertThat(logFiles).hasSize(4);
    }

    @Test
    void shouldReturnANegativeValueIfThereAreNoLogFiles() throws Throwable {
        // given
        LogFiles files = createLogFiles();

        fileSystem
                .write(databaseLayout.file(getVersionedLogFileName("some", "4")))
                .close();
        fileSystem.write(databaseLayout.file(DEFAULT_NAME)).close();

        // when
        final long highestLogVersion = files.getLogFile().getHighestLogVersion();

        // then
        assertEquals(-1, highestLogVersion);
        files.shutdown();
    }

    @Test
    void shouldFindTheVersionBasedOnTheFilename() throws Throwable {
        // given
        LogFiles logFiles = createLogFiles();
        final Path file = Path.of("v....2");

        // when
        long logVersion = logFiles.getLogFile().getLogVersion(file);

        // then
        assertEquals(2, logVersion);
        logFiles.shutdown();
    }

    @Test
    void shouldThrowIfThereIsNoVersionInTheFileName() throws Exception {
        LogFiles logFiles = createLogFiles();
        final Path file = Path.of("wrong");

        // when
        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> logFiles.getLogFile().getLogVersion(file));
        assertEquals("Invalid log file '" + file.getFileName() + "'", exception.getMessage());
    }

    @Test
    void shouldThrowIfVersionIsNotANumber() throws Exception {
        // given
        LogFiles logFiles = createLogFiles();
        final Path file = Path.of(getVersionedLogFileName("aa", "A"));

        // when
        assertThrows(NumberFormatException.class, () -> logFiles.getLogFile().getLogVersion(file));
    }

    @Test
    void isLogFile() throws Exception {
        LogFiles logFiles = createLogFiles();
        assertFalse(logFiles.isLogFile(Path.of("aaa.tx.log")));
        assertTrue(logFiles.isLogFile(Path.of(DEFAULT_NAME + ".0")));
        assertTrue(logFiles.isLogFile(Path.of(DEFAULT_NAME + ".17")));
        assertTrue(logFiles.isLogFile(Path.of("checkpoint.17")));
        assertFalse(logFiles.isLogFile(Path.of("thecheckpoint.17")));
    }

    @Test
    void emptyFileWithoutEntriesDoesNotHaveThem() throws Exception {
        LogFiles logFiles = createLogFiles();
        String file = getVersionedLogFileName("1");
        fileSystem.write(createTransactionLogFile(databaseLayout, file)).close();
        assertFalse(logFiles.getLogFile().hasAnyEntries(1));
    }

    @Test
    void fileWithoutEntriesDoesNotHaveThemIndependentlyOfItsSize() throws Exception {
        final var logFile = (TransactionLogFile) createLogFiles().getLogFile();
        try (PhysicalLogVersionedStoreChannel channel =
                logFile.createLogChannelForVersion(1, () -> 1L, LATEST_KERNEL_VERSION_PROVIDER, BASE_TX_CHECKSUM)) {
            assertThat(channel.size()).isGreaterThanOrEqualTo(LATEST_LOG_FORMAT.getHeaderSize());
            assertFalse(logFile.hasAnyEntries(1));
        }
    }

    private void create3_5FileWithHeader(DatabaseLayout databaseLayout, String version, int bytesOfData)
            throws IOException {
        try (StoreChannel storeChannel =
                fileSystem.write(createTransactionLogFile(databaseLayout, getVersionedLogFileName(version)))) {
            ByteBuffer byteBuffer =
                    ByteBuffers.allocate(V6.getHeaderSize() + bytesOfData, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            while (byteBuffer.hasRemaining()) {
                byteBuffer.put(V6.getVersionByte());
            }
            byteBuffer.flip();
            storeChannel.writeAll(byteBuffer);
        }
    }

    private static Path createTransactionLogFile(DatabaseLayout databaseLayout, String fileName) {
        Path transactionLogsDirectory = databaseLayout.getTransactionLogsDirectory();
        return transactionLogsDirectory.resolve(fileName);
    }

    private LogFiles createLogFiles() throws Exception {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        var files = LogFilesBuilder.builder(databaseLayout, fileSystem, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withLogVersionRepository(new SimpleLogVersionRepository())
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .build();
        files.init();
        return files;
    }

    private static String getVersionedLogFileName(int version) {
        return getVersionedLogFileName(DEFAULT_NAME, String.valueOf(version));
    }

    private static String getVersionedCheckpointLogFileName(int version) {
        return getVersionedLogFileName(CHECKPOINT_FILE_PREFIX, String.valueOf(version));
    }

    private static String getVersionedLogFileName(String version) {
        return getVersionedLogFileName(DEFAULT_NAME, version);
    }

    private static String getVersionedLogFileName(String filename, String version) {
        return filename + "." + version;
    }
}
