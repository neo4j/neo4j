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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryParserSets.parserSet;
import static org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor.EMPTY_ACCESSOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableChecksumChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.v56.LogEntryChunkEnd;
import org.neo4j.kernel.impl.transaction.log.entry.v56.LogEntryChunkStart;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class LogEntryParserDispatcherTest {
    private final KernelVersion version = LatestVersions.LATEST_KERNEL_VERSION;
    private final CommandReaderFactory commandReader = new TestCommandReaderFactory();
    private final LogPositionMarker marker = new LogPositionMarker();
    private final LogPosition position = new LogPosition(0, 25);

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Test
    void writeAndParseChunksEntries() throws IOException {
        assumeThat(LatestVersions.LATEST_KERNEL_VERSION).isGreaterThan(KernelVersion.V5_0);
        try (var buffer = new HeapScopedBuffer((int) kibiBytes(1), ByteOrder.LITTLE_ENDIAN, INSTANCE)) {
            Path path = directory.createFile("a");
            StoreChannel storeChannel = fs.write(path);
            try (PhysicalFlushableChecksumChannel writeChannel =
                    new PhysicalFlushableChecksumChannel(storeChannel, buffer)) {
                var entryWriter = new LogEntryWriter<>(writeChannel);
                byte version = LatestVersions.LATEST_KERNEL_VERSION.version();
                entryWriter.writeStartEntry(version, 1, 2, 3, EMPTY_BYTE_ARRAY);
                entryWriter.writeChunkEndEntry(version, 17, 13);
                entryWriter.writeChunkStartEntry(version, 11, 13, new LogPosition(14, 15));
                entryWriter.writeCommitEntry(version, 7, 8);
            }

            VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader(
                    StorageEngineFactory.defaultStorageEngine().commandReaderFactory(),
                    LatestVersions.LATEST_KERNEL_VERSION);
            try (var readChannel = new ReadAheadLogChannel(
                    new PhysicalLogVersionedStoreChannel(
                            fs.read(path), 1, (byte) -1, path, EMPTY_ACCESSOR, DatabaseTracer.NULL),
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
        final LogEntryStart start = new LogEntryStart(version, 1, 2, 3, new byte[] {4}, position);
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.putLong(start.getTimeWritten());
        channel.putLong(start.getLastCommittedTxWhenTransactionStarted());
        channel.putInt(start.getPreviousChecksum());
        channel.putInt(start.getAdditionalHeader().length);
        channel.put(start.getAdditionalHeader(), start.getAdditionalHeader().length);

        channel.getCurrentPosition(marker);

        // when
        final LogEntryParser parser = parserSet(
                        LatestVersions.LATEST_KERNEL_VERSION, LatestVersions.LATEST_KERNEL_VERSION)
                .select(LogEntryTypeCodes.TX_START);
        final LogEntry logEntry = parser.parse(version, channel, marker, commandReader);

        // then
        assertEquals(start, logEntry);
    }

    @Test
    void parseCommitEntry() throws IOException {
        // given
        final LogEntryCommit commit = new LogEntryCommit(42, 21, -361070784);
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.putLong(commit.getTxId());
        channel.putLong(commit.getTimeWritten());
        channel.putChecksum();

        channel.getCurrentPosition(marker);

        // when
        final LogEntryParser parser = parserSet(
                        LatestVersions.LATEST_KERNEL_VERSION, LatestVersions.LATEST_KERNEL_VERSION)
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
        channel.getCurrentPosition(marker);

        // when
        final LogEntryParser parser = parserSet(
                        LatestVersions.LATEST_KERNEL_VERSION, LatestVersions.LATEST_KERNEL_VERSION)
                .select(LogEntryTypeCodes.COMMAND);
        final LogEntry logEntry = parser.parse(version, channel, marker, commandReader);

        // then
        assertEquals(command, logEntry);
    }

    @Test
    void shouldThrowWhenParsingUnknownEntry() {
        assertThrows(IllegalArgumentException.class, () -> parserSet(
                        LatestVersions.LATEST_KERNEL_VERSION, LatestVersions.LATEST_KERNEL_VERSION)
                .select((byte) 42)); // unused, at lest for now
    }
}
