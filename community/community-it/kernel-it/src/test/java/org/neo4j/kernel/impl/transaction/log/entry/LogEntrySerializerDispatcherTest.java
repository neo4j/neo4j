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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newCommitEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newStartEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializationSets.serializationSet;
import static org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor.EMPTY_ACCESSOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableLogChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkEnd;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkStart;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class LogEntrySerializerDispatcherTest {
    private final KernelVersion version = LATEST_KERNEL_VERSION;
    private final CommandReaderFactory commandReader = TestCommandReaderFactory.INSTANCE;
    private final LogPositionMarker marker = new LogPositionMarker();
    private final LogPosition position = new LogPosition(0, 25);

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Test
    void writeAndParseChunksEntries() throws IOException {
        try (var buffer = new HeapScopedBuffer((int) kibiBytes(1), ByteOrder.LITTLE_ENDIAN, INSTANCE)) {
            Path path = directory.createFile("a");
            StoreChannel storeChannel = fs.write(path);
            try (PhysicalFlushableLogChannel writeChannel = new PhysicalFlushableLogChannel(storeChannel, buffer)) {
                var entryWriter = new LogEntryWriter<>(writeChannel, LatestVersions.BINARY_VERSIONS);
                entryWriter.writeStartEntry(LATEST_KERNEL_VERSION, 1, 2, 3, EMPTY_BYTE_ARRAY);
                entryWriter.writeChunkEndEntry(LATEST_KERNEL_VERSION, 17, 13);
                entryWriter.writeChunkStartEntry(LATEST_KERNEL_VERSION, 11, 13, new LogPosition(14, 15));
                entryWriter.writeCommitEntry(LATEST_KERNEL_VERSION, 7, 8);
            }

            VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader(
                    StorageEngineFactory.defaultStorageEngine().commandReaderFactory(), LatestVersions.BINARY_VERSIONS);
            try (var readChannel = new ReadAheadLogChannel(
                    new PhysicalLogVersionedStoreChannel(
                            fs.read(path), 1, LATEST_LOG_FORMAT, path, EMPTY_ACCESSOR, DatabaseTracer.NULL),
                    NO_MORE_CHANNELS,
                    INSTANCE)) {
                var startEntry = entryReader.readLogEntry(readChannel);
                var chunkEnd = entryReader.readLogEntry(readChannel);
                var chunkStart = entryReader.readLogEntry(readChannel);
                var commitEnd = entryReader.readLogEntry(readChannel);

                assertThat(startEntry).isInstanceOf(LogEntryStart.class);
                assertThat(commitEnd).isInstanceOf(LogEntryCommit.class);

                assertThat(chunkEnd).isInstanceOf(LogEntryChunkEnd.class);
                var chunkEndEntry = (LogEntryChunkEnd) chunkEnd;
                assertEquals(17, chunkEndEntry.getTransactionId());
                assertEquals(13, chunkEndEntry.getChunkId());

                assertThat(chunkStart).isInstanceOf(LogEntryChunkStart.class);
                var chunkStartEntry = (LogEntryChunkStart) chunkStart;
                assertEquals(11, chunkStartEntry.getTimeWritten());
                assertEquals(13, chunkStartEntry.getChunkId());
            }
        }
    }

    @Test
    void parseStartEntry() throws IOException {
        // given
        final LogEntryStart start = newStartEntry(version, 1, 2, 3, new byte[] {4}, position);
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.putLong(start.getTimeWritten());
        channel.putLong(start.getLastCommittedTxWhenTransactionStarted());
        channel.putInt(start.getPreviousChecksum());
        channel.putInt(start.getAdditionalHeader().length);
        channel.put(start.getAdditionalHeader(), start.getAdditionalHeader().length);

        channel.getCurrentLogPosition(marker);

        // when
        final LogEntrySerializer parser = serializationSet(LATEST_KERNEL_VERSION, LatestVersions.BINARY_VERSIONS)
                .select(LogEntryTypeCodes.TX_START);
        final LogEntry logEntry = parser.parse(version, channel, marker, commandReader);

        // then
        assertEquals(start, logEntry);
    }

    @Test
    void parseCommitEntry() throws IOException {
        // given
        final LogEntryCommit commit = newCommitEntry(LATEST_KERNEL_VERSION, 42, 21, -361070784);
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.putLong(commit.getTxId());
        channel.putLong(commit.getTimeWritten());
        channel.putChecksum();

        channel.getCurrentLogPosition(marker);

        // when
        final LogEntrySerializer parser = serializationSet(LATEST_KERNEL_VERSION, LatestVersions.BINARY_VERSIONS)
                .select(LogEntryTypeCodes.TX_COMMIT);
        final LogEntry logEntry = parser.parse(version, channel, marker, commandReader);

        // then
        assertEquals(commit, logEntry);
    }

    @Test
    void shouldParserCommandsUsingAGivenFactory() throws IOException {
        // given
        // The record, it will be used as before and after
        TestCommand testCommand = new TestCommand(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9});
        final LogEntryCommand command = new LogEntryCommand(testCommand);
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();
        testCommand.serialize(channel);
        channel.getCurrentLogPosition(marker);

        // when
        final LogEntrySerializer parser = serializationSet(LATEST_KERNEL_VERSION, LatestVersions.BINARY_VERSIONS)
                .select(LogEntryTypeCodes.COMMAND);
        final LogEntry logEntry = parser.parse(version, channel, marker, commandReader);

        // then
        assertEquals(command, logEntry);
    }

    @Test
    void shouldThrowWhenParsingUnknownEntry() {
        assertThrows(IllegalArgumentException.class, () -> serializationSet(
                        LATEST_KERNEL_VERSION, LatestVersions.BINARY_VERSIONS)
                .select((byte) 42)); // unused, at lest for now
    }
}
