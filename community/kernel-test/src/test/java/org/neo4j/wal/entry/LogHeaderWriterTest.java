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
package org.neo4j.wal.entry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_ID;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.wal.entry.LogFormat.LOG_VERSION_MASK;
import static org.neo4j.wal.entry.LogFormat.V10;
import static org.neo4j.wal.entry.LogFormat.V11;
import static org.neo4j.wal.entry.LogFormat.V9;
import static org.neo4j.wal.entry.LogFormat.encodeLogVersion;
import static org.neo4j.wal.entry.LogFormat.writeLogHeader;
import static org.neo4j.wal.entry.LogHeader.UNSPECIFIED_CREATION_TIME;
import static org.neo4j.wal.entry.LogHeaderReader.decodeLogFormatVersion;
import static org.neo4j.wal.entry.LogHeaderReader.decodeLogVersion;
import static org.neo4j.wal.entry.LogSegments.UNKNOWN_LOG_SEGMENT_SIZE;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdSerialization;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@RandomSupportExtension
class LogHeaderWriterTest {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private RandomSupport random;

    private long expectedLogVersion;
    private long expectedAppendIndex;
    private StoreIdentifier expectedStoreId;
    private int expectedBlockSize;
    private int expectedChecksum;
    private long expectedTerm;

    @BeforeEach
    void setUp() {
        expectedLogVersion = random.nextLong(0, LOG_VERSION_MASK);
        expectedAppendIndex = random.nextLong(0, Long.MAX_VALUE);
        expectedStoreId = StoreIdentifier.newStoreIdentifier(new StoreId(
                random.nextLong(),
                random.nextLong(),
                "engine-" + random.nextInt(0, 255),
                "format-" + random.nextInt(0, 255),
                random.nextInt(0, 127),
                random.nextInt(0, 127)));
        expectedBlockSize = 1 << random.nextInt(7, 10);
        expectedChecksum = random.nextInt();
        expectedTerm = random.nextLong(0, Long.MAX_VALUE);
    }

    @ParameterizedTest
    @EnumSource(
            mode = EXCLUDE,
            names = {"V6", "V7"}) // We don't support writing v6 and v7
    void shouldWriteALogHeaderInAStoreChannel(LogFormat logFormat) throws IOException {
        // given
        final var file = testDirectory.file("WriteLogHeader");
        final var channel = fileSystem.write(file);
        LogHeader logHeader = logFormat.newHeader(
                expectedLogVersion,
                expectedAppendIndex,
                expectedTerm,
                expectedStoreId,
                logFormat != V10 ? UNKNOWN_LOG_SEGMENT_SIZE : expectedBlockSize,
                expectedChecksum,
                KernelVersion.GLORIOUS_FUTURE,
                UNSPECIFIED_CREATION_TIME,
                UNKNOWN_TX_ID);

        // when
        writeLogHeader(channel, logHeader, INSTANCE);

        channel.close();

        // then
        final var array = new byte[logFormat.getHeaderSize()];
        try (var stream = fileSystem.openAsInputStream(file)) {
            assertEquals(logFormat.getHeaderSize(), stream.read(array));
        }
        final var result = ByteBuffer.wrap(array);

        final var encodedLogVersions = result.getLong();
        final var txId = V10.compareTo(logFormat) > 0 ? result.getLong() : -1;
        final var appendIndex = V9.compareTo(logFormat) <= 0 ? result.getLong() : -1;
        StoreIdentifier storeId;
        if (logFormat == LogFormat.V11) {
            storeId = StoreIdentifier.newStoreIdentifier(result.getLong());
        } else {
            storeId = StoreIdentifier.newStoreIdentifier(StoreIdSerialization.deserializeWithFixedSize(result));
        }

        assertEquals(encodeLogVersion(expectedLogVersion, logFormat.getVersionByte()), encodedLogVersions);
        assertEquals(logFormat.getVersionByte(), decodeLogFormatVersion(encodedLogVersions));
        assertEquals(expectedLogVersion, decodeLogVersion(encodedLogVersions));
        if (V9.compareTo(logFormat) <= 0) {
            assertEquals(expectedAppendIndex, appendIndex);
        } else if (V10.compareTo(logFormat) > 0) {
            assertEquals(expectedAppendIndex, txId);
        }
        assertTrue(storeId.matches(expectedStoreId.storeId()));

        if (logFormat == V10) {
            assertEquals(expectedBlockSize, result.getInt());
            assertEquals(expectedChecksum, result.getInt());
            assertEquals(expectedTerm, result.getLong());
        }
    }

    @Test
    void shouldWriteEveryV11FieldAtItsOffset() throws IOException {
        long creationTime = random.nextLong(1, Long.MAX_VALUE);
        long lastTxId = random.nextLong(1, Long.MAX_VALUE);
        final var file = testDirectory.file("WriteLogHeader");
        LogHeader logHeader = V11.newHeader(
                expectedLogVersion,
                expectedAppendIndex,
                expectedTerm,
                expectedStoreId,
                expectedBlockSize,
                expectedChecksum,
                LATEST_KERNEL_VERSION,
                creationTime,
                lastTxId);
        try (var channel = fileSystem.write(file)) {
            writeLogHeader(channel, logHeader, INSTANCE);
        }

        final var array = new byte[V11.getHeaderSize()];
        try (var stream = fileSystem.openAsInputStream(file)) {
            assertEquals(V11.getHeaderSize(), stream.read(array));
        }
        final var result = ByteBuffer.wrap(array);

        assertEquals(encodeLogVersion(expectedLogVersion, V11.getVersionByte()), result.getLong());
        assertEquals(expectedAppendIndex, result.getLong());
        assertEquals(expectedStoreId.randomValueIdentifier(), result.getLong());
        assertEquals(expectedBlockSize, result.getInt());
        assertEquals(expectedChecksum, result.getInt());
        assertEquals(expectedTerm, result.getLong());
        assertEquals(LATEST_KERNEL_VERSION.version(), result.get());
        assertEquals(creationTime, result.getLong());
        assertEquals(lastTxId, result.getLong());
        while (result.hasRemaining()) {
            assertEquals(0, result.get());
        }
    }
}
