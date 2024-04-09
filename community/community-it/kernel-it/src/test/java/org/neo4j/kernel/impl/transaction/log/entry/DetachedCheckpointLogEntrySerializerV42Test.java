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

import static java.lang.Math.min;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.Numbers.safeCastIntToShort;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializer.writeLogEntryHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.DETACHED_CHECK_POINT;
import static org.neo4j.kernel.impl.transaction.log.entry.v42.DetachedCheckpointLogEntrySerializerV4_2.MAX_DESCRIPTION_LENGTH;
import static org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor.EMPTY_ACCESSOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Arrays;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableLogChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.v42.LogEntryDetachedCheckpointV4_2;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class DetachedCheckpointLogEntrySerializerV42Test {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    private final CommandReaderFactory commandReader = TestCommandReaderFactory.INSTANCE;
    private final LogPositionMarker positionMarker = new LogPositionMarker();

    @Test
    void parseDetachedCheckpointRecord() throws IOException {
        KernelVersion version = KernelVersion.V4_3_D4;
        var storeId = new StoreId(4, 5, "legacy", "legacy", 1, 1);
        var channel = new InMemoryClosableChannel();
        int checkpointMillis = 3;
        String checkpointDescription = "checkpoint";
        byte[] bytes = Arrays.copyOf(checkpointDescription.getBytes(), 120);
        // For version confusion, please read LogEntryParserSetV4_3 comments
        var checkpoint = new LogEntryDetachedCheckpointV4_2(
                version, new LogPosition(1, 2), checkpointMillis, storeId, checkpointDescription);

        writeLogEntryHeader(version, DETACHED_CHECK_POINT, channel);
        channel.putLong(checkpoint.getLogPosition().getLogVersion())
                .putLong(checkpoint.getLogPosition().getByteOffset())
                .putLong(checkpointMillis)
                .putLong(storeId.getCreationTime())
                .putLong(storeId.getRandom())
                .putLong(123)
                .putLong(0) // legacy upgrade timestamp
                .putLong(0) // legacy upgrade tx id
                .putShort((short) checkpointDescription.getBytes().length)
                .put(bytes, bytes.length);
        channel.putChecksum();

        assertEquals(version.version(), channel.getVersion());
        assertEquals(DETACHED_CHECK_POINT, channel.get());
        var checkpointParser = LogEntrySerializationSets.serializationSet(version, LatestVersions.BINARY_VERSIONS)
                .select(DETACHED_CHECK_POINT);
        LogEntry logEntry = checkpointParser.parse(version, channel, positionMarker, commandReader);
        assertEquals(checkpoint, logEntry);
    }

    @Test
    void writeAndParseCheckpointKernelVersion() throws IOException {
        Path path = directory.createFile("a");

        try (var buffer = new HeapScopedBuffer((int) kibiBytes(1), ByteOrder.BIG_ENDIAN, INSTANCE)) {
            StoreChannel storeChannel = fs.write(path);
            try (PhysicalFlushableLogChannel writeChannel = new PhysicalFlushableLogChannel(storeChannel, buffer)) {
                writeCheckpoint(writeChannel, KernelVersion.V4_4, StringUtils.repeat("b", 1024));
            }
        }
        try (var buffer = new HeapScopedBuffer((int) kibiBytes(1), ByteOrder.LITTLE_ENDIAN, INSTANCE)) {
            StoreChannel storeChannel = fs.open(path, Set.of(StandardOpenOption.WRITE, StandardOpenOption.APPEND));
            try (PhysicalFlushableLogChannel writeChannel = new PhysicalFlushableLogChannel(storeChannel, buffer)) {
                writeCheckpoint(writeChannel, KernelVersion.V5_0, StringUtils.repeat("c", 1024));
            }
        }

        VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader(
                StorageEngineFactory.defaultStorageEngine().commandReaderFactory(), LatestVersions.BINARY_VERSIONS);
        try (var readChannel = new ReadAheadLogChannel(
                new PhysicalLogVersionedStoreChannel(
                        fs.read(path), -1 /* ignored */, LATEST_LOG_FORMAT, path, EMPTY_ACCESSOR, DatabaseTracer.NULL),
                NO_MORE_CHANNELS,
                INSTANCE)) {
            assertEquals(
                    KernelVersion.V4_4, readCheckpoint(entryReader, readChannel).kernelVersion());
            assertEquals(
                    KernelVersion.V5_0, readCheckpoint(entryReader, readChannel).kernelVersion());
        }
    }

    private LogEntryDetachedCheckpointV4_2 readCheckpoint(
            VersionAwareLogEntryReader entryReader, ReadAheadLogChannel readChannel) throws IOException {
        return (LogEntryDetachedCheckpointV4_2) entryReader.readLogEntry(readChannel);
    }

    private static void writeCheckpoint(WritableChannel channel, KernelVersion kernelVersion, String reason)
            throws IOException {
        var storeId = new StoreId(4, 5, "engine-1", "format-1", 1, 2);
        var logPosition = new LogPosition(1, 2);

        writeCheckPointEntry(channel, kernelVersion, logPosition, Instant.ofEpochMilli(1), storeId, reason);
    }

    private static void writeCheckPointEntry(
            WritableChannel channel,
            KernelVersion kernelVersion,
            LogPosition logPosition,
            Instant checkpointTime,
            StoreId storeId,
            String reason)
            throws IOException {
        channel.beginChecksumForWriting();
        writeLogEntryHeader(kernelVersion, DETACHED_CHECK_POINT, channel);
        byte[] reasonBytes = reason.getBytes();
        short length = safeCastIntToShort(min(reasonBytes.length, MAX_DESCRIPTION_LENGTH));
        byte[] descriptionBytes = new byte[MAX_DESCRIPTION_LENGTH];
        System.arraycopy(reasonBytes, 0, descriptionBytes, 0, length);
        channel.putLong(logPosition.getLogVersion())
                .putLong(logPosition.getByteOffset())
                .putLong(checkpointTime.toEpochMilli())
                .putLong(storeId.getCreationTime())
                .putLong(storeId.getRandom())
                .putLong(123)
                .putLong(0)
                .putLong(0);
        channel.putShort(length).put(descriptionBytes, descriptionBytes.length);
        channel.putChecksum();
    }
}
