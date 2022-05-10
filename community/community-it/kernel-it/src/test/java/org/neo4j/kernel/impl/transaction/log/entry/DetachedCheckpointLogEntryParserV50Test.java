/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.entry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.DETACHED_CHECK_POINT_V5_0;
import static org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor.EMPTY_ACCESSOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.Instant;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableChecksumChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.WritableChecksumChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.entry.v50.LogEntryDetachedCheckpointV5_0;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class DetachedCheckpointLogEntryParserV50Test {

    private static final StoreId TEST_STORE_ID = new StoreId(1, 1, "engine-1", "format-1", 1, 1);

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Test
    void writeAndParseCheckpointKernelVersion() throws IOException {
        try (var buffer = new HeapScopedBuffer((int) kibiBytes(1), ByteOrder.LITTLE_ENDIAN, INSTANCE)) {
            Path path = directory.createFile("a");
            StoreChannel storeChannel = fs.write(path);
            try (PhysicalFlushableChecksumChannel writeChannel =
                    new PhysicalFlushableChecksumChannel(storeChannel, buffer)) {
                writeCheckpoint(writeChannel, KernelVersion.V5_0, StringUtils.repeat("c", 1024));
            }

            VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader(
                    StorageEngineFactory.defaultStorageEngine().commandReaderFactory());
            try (var readChannel = new ReadAheadLogChannel(
                    new PhysicalLogVersionedStoreChannel(
                            fs.read(path), -1 /* ignored */, (byte) -1, path, EMPTY_ACCESSOR, DatabaseTracer.NULL),
                    NO_MORE_CHANNELS,
                    INSTANCE)) {
                var checkpointV50 = readCheckpoint(entryReader, readChannel);
                assertEquals(DETACHED_CHECK_POINT_V5_0, checkpointV50.getType());
                assertEquals(KernelVersion.V5_0, checkpointV50.getVersion());
                assertEquals(new LogPosition(1, 2), checkpointV50.getLogPosition());
                assertEquals(TEST_STORE_ID, checkpointV50.getStoreId());
                assertEquals(new TransactionId(100, 101, 102), checkpointV50.getTransactionId());
            }
        }
    }

    @Test
    void failToParse50CheckpointWithOlderKernelVersion() throws IOException {
        try (var buffer = new HeapScopedBuffer((int) kibiBytes(1), ByteOrder.LITTLE_ENDIAN, INSTANCE)) {
            Path path = directory.createFile("a");
            StoreChannel storeChannel = fs.write(path);
            try (PhysicalFlushableChecksumChannel writeChannel =
                    new PhysicalFlushableChecksumChannel(storeChannel, buffer)) {
                writeCheckpoint(writeChannel, KernelVersion.V4_4, StringUtils.repeat("c", 1024));
            }

            VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader(
                    StorageEngineFactory.defaultStorageEngine().commandReaderFactory());
            try (var readChannel = new ReadAheadLogChannel(
                    new PhysicalLogVersionedStoreChannel(
                            fs.read(path), -1 /* ignored */, (byte) -1, path, EMPTY_ACCESSOR, DatabaseTracer.NULL),
                    NO_MORE_CHANNELS,
                    INSTANCE)) {
                assertThrows(IOException.class, () -> readCheckpoint(entryReader, readChannel));
            }
        }
    }

    private LogEntryDetachedCheckpointV5_0 readCheckpoint(
            VersionAwareLogEntryReader entryReader, ReadableClosablePositionAwareChecksumChannel readChannel)
            throws IOException {
        return (LogEntryDetachedCheckpointV5_0) entryReader.readLogEntry(readChannel);
    }

    private static void writeCheckpoint(WritableChecksumChannel channel, KernelVersion kernelVersion, String reason)
            throws IOException {
        var logPosition = new LogPosition(1, 2);
        var transactionId = new TransactionId(100, 101, 102);

        writeCheckPointEntry(
                channel, kernelVersion, transactionId, logPosition, Instant.ofEpochMilli(1), TEST_STORE_ID, reason);
    }

    private static void writeCheckPointEntry(
            WritableChecksumChannel channel,
            KernelVersion kernelVersion,
            TransactionId transactionId,
            LogPosition logPosition,
            Instant checkpointTime,
            StoreId storeId,
            String reason)
            throws IOException {
        new DetachedCheckpointLogEntryWriter(channel, () -> kernelVersion)
                .writeCheckPointEntry(transactionId, logPosition, checkpointTime, storeId, reason);
    }
}
