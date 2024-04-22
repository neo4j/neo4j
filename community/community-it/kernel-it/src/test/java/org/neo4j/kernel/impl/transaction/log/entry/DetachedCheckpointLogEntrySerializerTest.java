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
package org.neo4j.kernel.impl.transaction.log.entry;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializationSets.serializationSet;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.DETACHED_CHECK_POINT_V5_0;
import static org.neo4j.kernel.impl.transaction.log.entry.v50.DetachedCheckpointLogEntrySerializerV5_0.RECORD_LENGTH_BYTES;
import static org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor.EMPTY_ACCESSOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.StoreIdSerialization.MAX_STORE_ID_LENGTH;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableLogChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.v50.LogEntryDetachedCheckpointV5_0;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryDetachedCheckpointV5_20;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdSerialization;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.arguments.KernelVersionSource;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class DetachedCheckpointLogEntrySerializerTest {

    private static final StoreId TEST_STORE_ID = new StoreId(3, 4, "engine-1", "format-1", 11, 22);

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Test
    void writeAndParseCheckpointKernelVersion() throws IOException {
        try (var buffer = new HeapScopedBuffer((int) kibiBytes(1), ByteOrder.LITTLE_ENDIAN, INSTANCE)) {
            Path path = directory.createFile("a");
            StoreChannel storeChannel = fs.write(path);
            try (PhysicalFlushableLogChannel writeChannel = new PhysicalFlushableLogChannel(storeChannel, buffer)) {
                writeCheckpoint(writeChannel, KernelVersion.V5_0, StringUtils.repeat("c", 1024));
            }

            VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader(
                    StorageEngineFactory.defaultStorageEngine().commandReaderFactory(), LatestVersions.BINARY_VERSIONS);
            try (var readChannel = new ReadAheadLogChannel(
                    new PhysicalLogVersionedStoreChannel(
                            fs.read(path),
                            -1 /* ignored */,
                            LATEST_LOG_FORMAT,
                            path,
                            EMPTY_ACCESSOR,
                            DatabaseTracer.NULL),
                    NO_MORE_CHANNELS,
                    INSTANCE)) {
                var checkpointV50 = readCheckpoint(entryReader, readChannel);
                assertEquals(DETACHED_CHECK_POINT_V5_0, checkpointV50.getType());
                assertEquals(KernelVersion.V5_0, checkpointV50.kernelVersion());
                assertEquals(new LogPosition(100, 200), checkpointV50.getLogPosition());
                assertEquals(TEST_STORE_ID, checkpointV50.getStoreId());
                assertEquals(
                        new TransactionId(70, 70, KernelVersion.V5_0, 80, 90, UNKNOWN_CONSENSUS_INDEX),
                        checkpointV50.getTransactionId());
            }
        }
    }

    @Test
    void failToParse50CheckpointWithOlderKernelVersion() throws IOException {
        try (var buffer = new HeapScopedBuffer((int) kibiBytes(1), ByteOrder.LITTLE_ENDIAN, INSTANCE)) {
            Path path = directory.createFile("a");
            StoreChannel storeChannel = fs.write(path);
            try (PhysicalFlushableLogChannel writeChannel = new PhysicalFlushableLogChannel(storeChannel, buffer)) {
                byte[] storeIdBuffer = new byte[MAX_STORE_ID_LENGTH];
                StoreIdSerialization.serializeWithFixedSize(TEST_STORE_ID, ByteBuffer.wrap(storeIdBuffer));
                writeChannel
                        .put(KernelVersion.V4_4.version())
                        .put(DETACHED_CHECK_POINT_V5_0)
                        .putLong(1)
                        .putLong(2)
                        .putLong(3)
                        .put(storeIdBuffer, storeIdBuffer.length)
                        .putLong(4)
                        .putInt(5)
                        .putLong(6)
                        .putShort((short) 4)
                        .put(new byte[] {7, 8, 9, 10}, 4);
                writeChannel.putChecksum();
            }

            VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader(
                    StorageEngineFactory.defaultStorageEngine().commandReaderFactory(), LatestVersions.BINARY_VERSIONS);
            try (var readChannel = new ReadAheadLogChannel(
                    new PhysicalLogVersionedStoreChannel(
                            fs.read(path), 1, LogFormat.V7, path, EMPTY_ACCESSOR, DatabaseTracer.NULL),
                    NO_MORE_CHANNELS,
                    INSTANCE)) {
                assertThatThrownBy(() -> readCheckpoint(entryReader, readChannel))
                        .rootCause()
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void detachedCheckpointEntryHasSpecificLength(KernelVersion kernelVersion) throws IOException {
        try (var buffer = new HeapScopedBuffer((int) kibiBytes(1), ByteOrder.LITTLE_ENDIAN, INSTANCE)) {
            StoreChannel storeChannel = fs.write(directory.createFile("a"));
            try (PhysicalFlushableLogChannel writeChannel = new PhysicalFlushableLogChannel(storeChannel, buffer)) {
                long initialPosition = writeChannel.position();
                writeCheckpoint(writeChannel, kernelVersion, "checkpoint reason");

                assertThat(writeChannel.position() - initialPosition).isEqualTo(RECORD_LENGTH_BYTES);
            }
        }
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void anyCheckpointEntryHaveTheSameSize(KernelVersion kernelVersion) throws IOException {
        try (var buffer = new HeapScopedBuffer((int) kibiBytes(1), ByteOrder.LITTLE_ENDIAN, INSTANCE)) {
            StoreChannel storeChannel = fs.write(directory.createFile("b"));
            try (PhysicalFlushableLogChannel writeChannel = new PhysicalFlushableLogChannel(storeChannel, buffer)) {

                for (int i = 0; i < 100; i++) {
                    long initialPosition = writeChannel.position();
                    writeCheckpoint(writeChannel, kernelVersion, randomAlphabetic(10, 512));
                    long recordLength = writeChannel.position() - initialPosition;
                    assertThat(recordLength).isEqualTo(RECORD_LENGTH_BYTES);
                }
            }
        }
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void longCheckpointReasonIsTrimmedToFit(KernelVersion kernelVersion) throws IOException {
        try (var buffer = new HeapScopedBuffer((int) kibiBytes(1), ByteOrder.LITTLE_ENDIAN, INSTANCE)) {
            StoreChannel storeChannel = fs.write(directory.createFile("b"));
            try (PhysicalFlushableLogChannel writeChannel = new PhysicalFlushableLogChannel(storeChannel, buffer)) {

                long initialPosition = writeChannel.position();
                writeCheckpoint(writeChannel, kernelVersion, StringUtils.repeat("b", 1024));
                long recordLength = writeChannel.position() - initialPosition;
                assertThat(recordLength).isEqualTo(RECORD_LENGTH_BYTES);
            }
        }
    }

    private static void writeCheckpoint(WritableChannel channel, KernelVersion kernelVersion, String reason)
            throws IOException {
        var transactionId = new TransactionId(70, 70, LATEST_KERNEL_VERSION, 80, 90, 10);
        LogPosition logPosition = new LogPosition(100, 200);
        serializationSet(kernelVersion, LatestVersions.BINARY_VERSIONS)
                .select(LogEntryTypeCodes.DETACHED_CHECK_POINT_V5_0)
                .write(channel, checkpointEntry(kernelVersion, reason, transactionId, logPosition));
    }

    private static AbstractVersionAwareLogEntry checkpointEntry(
            KernelVersion kernelVersion, String reason, TransactionId transactionId, LogPosition logPosition) {
        if (kernelVersion.isAtLeast(KernelVersion.VERSION_APPEND_INDEX_INTRODUCED)) {
            return new LogEntryDetachedCheckpointV5_20(
                    kernelVersion,
                    transactionId,
                    transactionId.appendIndex() + 7,
                    logPosition,
                    1,
                    TEST_STORE_ID,
                    reason);
        }
        return new LogEntryDetachedCheckpointV5_0(kernelVersion, transactionId, logPosition, 1, TEST_STORE_ID, reason);
    }

    private LogEntryDetachedCheckpointV5_0 readCheckpoint(
            VersionAwareLogEntryReader entryReader, ReadableLogPositionAwareChannel readChannel) throws IOException {
        return (LogEntryDetachedCheckpointV5_0) entryReader.readLogEntry(readChannel);
    }
}
