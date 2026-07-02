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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED;
import static org.neo4j.kernel.KernelVersionProviders.fixed;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.V10;
import static org.neo4j.kernel.impl.transaction.log.entry.LogSegments.DEFAULT_LOG_SEGMENT_SIZE;
import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.CHECKPOINT_FILE_PREFIX;
import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.DEFAULT_NAME;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION_PROVIDER;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT_PROVIDER;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.LogTracers;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.enveloped.EnvelopeWriteChannel;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.storageengine.api.StoreId;
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
        final long highestLogVersion = files.getLogFile().getLogRangeInfo().highestVersion();

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
                .write(databaseLayout.file(getVersionedLogFileName("some", "4")).baseSegment())
                .close();
        fileSystem.write(databaseLayout.file(DEFAULT_NAME).baseSegment()).close();

        // when
        final long highestLogVersion = files.getLogFile().getLogRangeInfo().highestVersion();

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
        assertThatCode(() -> logFiles.getLogFile().getLogVersion(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid sequential file 'wrong'");
    }

    @Test
    void shouldThrowIfVersionIsNotANumber() throws Exception {
        // given
        LogFiles logFiles = createLogFiles();
        final Path file = Path.of(getVersionedLogFileName("aa", "A"));

        // when
        assertThatCode(() -> logFiles.getLogFile().getLogVersion(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not have a valid version suffix");
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
        try (PhysicalLogVersionedStoreChannel channel = logFile.createLogChannelForVersion(
                1, () -> 1L, LATEST_KERNEL_VERSION_PROVIDER, BASE_TX_CHECKSUM, LATEST_LOG_FORMAT_PROVIDER)) {
            assertThat(channel.size()).isGreaterThanOrEqualTo(LATEST_LOG_FORMAT.getHeaderSize());
            assertFalse(logFile.hasAnyEntries(1));
        }
    }

    @Test
    void envelopedFileWithZeroChecksumHasEntries() throws Exception {
        KernelVersion kernelVersion = VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED;
        LogFile logFile = createLogFiles(fixed(kernelVersion)).getLogFile();

        int checksum = writeEntryAndGetChecksum(logFile, kernelVersion, 0);
        int payload = adjustCRC32C(checksum, 0);
        checksum = writeEntryAndGetChecksum(logFile, kernelVersion, payload);
        assertThat(checksum).isEqualTo(0);

        assertTrue(logFile.hasAnyEntries(1));
    }

    private int writeEntryAndGetChecksum(LogFile logFile, KernelVersion kernelVersion, int payload) throws IOException {
        try (PhysicalLogVersionedStoreChannel channel = logFile.createLogChannelForVersion(
                        1, () -> 1L, fixed(kernelVersion), BASE_TX_CHECKSUM, () -> V10);
                EnvelopeWriteChannel envelopeWriteChannel = getEnvelopeChannel(channel)) {
            envelopeWriteChannel.beginChecksumForWriting();
            envelopeWriteChannel.putVersion(kernelVersion.version());
            envelopeWriteChannel.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
            envelopeWriteChannel.putInt(payload);
            envelopeWriteChannel.endCurrentEntry();
            return envelopeWriteChannel.currentChecksum();
        }
    }

    private static EnvelopeWriteChannel getEnvelopeChannel(PhysicalLogVersionedStoreChannel channel)
            throws IOException {
        return new EnvelopeWriteChannel(
                channel,
                new NativeScopedBuffer(DEFAULT_LOG_SEGMENT_SIZE, ByteOrder.LITTLE_ENDIAN, INSTANCE),
                DEFAULT_LOG_SEGMENT_SIZE,
                BASE_TX_CHECKSUM,
                1,
                LogEnvelopeHeader.UNSPECIFIED_TERM,
                LogTracers.NULL,
                LogRotation.NO_ROTATION);
    }

    private static Path createTransactionLogFile(DatabaseLayout databaseLayout, String fileName) {
        Path transactionLogsDirectory = databaseLayout.getTransactionLogsDirectory();
        return transactionLogsDirectory.resolve(fileName);
    }

    private LogFiles createLogFiles() throws Exception {
        return createLogFiles(LATEST_KERNEL_VERSION_PROVIDER);
    }

    private LogFiles createLogFiles(KernelVersionProvider kvp) throws Exception {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        var files = LogFilesBuilder.writeableBuilder(
                        databaseLayout, fileSystem, kvp, () -> LogFormat.fromKernelVersion(kvp.kernelVersion()))
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

    /**
     * Calculate the payload needed to generate a particular CRC32C.
     *
     * <pre>
     *     We need to find a P that will satisfy
     *     CRC(M || P) = target
     *
     *     Applying some GF(2^n) rules
     *     CRC((M || 0000) XOR P) = target
     *     CRC(M || 0000) XOR CRC(P) = target
     *     CRC(P) = target XOR CRC(M || 0000) = K
     *
     *     We also know that
     *     CRC(P) = P * x^32 mod crc-poly
     *     P = K * inverse(x^32) mod crc-poly
     *
     *     We can pre-calculate the inverse of x^32 for the CRC32C polynomial
     *     inverse(x^32) mod crc-poly = inverse(0x100000000) mod 0x11EDC6F41L = 0x7E6b086BL
     *
     *     And final formula is thus
     *     K = target XOR CRC(M || 0000)
     *     P = K * 0x7E6b086BL mod 0x11EDC6F41L
     * </pre>
     *
     * @param currentCRC The crc of the payload with 4 trailing zero bytes.
     * @param target The target crc.
     * @return payload to replace the 4 zeroes at the end.
     */
    private static int adjustCRC32C(int currentCRC, long target) {
        // Work with longs so we can do unsigned integer operations
        long k = Long.reverse(target ^ Integer.toUnsignedLong(currentCRC)) >>> 32;
        long multiply = gf2nMultiply(k, 0x7E6b086BL, 0x11EDC6F41L);
        return (int) (Long.reverse(multiply) >>> 32);
    }

    private static long gf2nMultiply(long x, long y, long polynomial) {
        long z = 0L;
        while (y != 0L) {
            z ^= x * (y & 1L);
            y >>>= 1L;
            x <<= 1L;
            if (((x >>> 32) & 1L) != 0L) {
                x ^= polynomial;
            }
        }
        return z;
    }
}
