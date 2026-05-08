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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.fs.ChannelNativeAccessor.EMPTY_ACCESSOR;
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;
import static org.neo4j.kernel.impl.transaction.log.LogIndexEncoding.encodeLogIndex;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newCommitEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newStartEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializationSets.serializationSet;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_SEQUENCE_NUMBER;
import static org.neo4j.test.LatestVersions.BINARY_VERSIONS;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadUtils;
import org.neo4j.kernel.impl.transaction.log.entry.v42.LogEntryStartV4_2;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryChunkEnd;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryChunkStart;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryStartV5_20;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.arguments.KernelVersionSource;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class LogEntrySerializerDispatcherTest {
    private final CommandReaderFactory commandReader = TestCommandReaderFactory.INSTANCE;
    private final LogPositionMarker marker = new LogPositionMarker();

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.20") // revised so that chunks now added in '5.20'
    void writeAndParseChunksEntries(KernelVersion version) throws IOException {
        Path path = directory.createFile("a");
        StoreChannel storeChannel = fs.write(path);
        LogFormat logFormat = LogFormat.fromKernelVersion(version);
        LogHeader logHeader = logFormat.newHeader(
                1,
                2,
                LogHeader.UNKNOWN_TERM,
                StoreIdentifier.UNKNOWN,
                logFormat.getDefaultSegmentBlockSize(),
                BASE_TX_CHECKSUM,
                version);
        LogFormat.writeLogHeader(storeChannel, logHeader, INSTANCE);
        try (PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                        storeChannel, logHeader, path, EMPTY_ACCESSOR, DatabaseTracer.NULL);
                var writeChannel =
                        new PhysicalFlushableLogPositionAwareChannel(versionedStoreChannel, logHeader, INSTANCE)) {
            var entryWriter = new LogEntryWriter<>(writeChannel, BINARY_VERSIONS);
            entryWriter.writeStartEntry(
                    version, 1, 2, 3, UNKNOWN_TX_SEQUENCE_NUMBER, 4, NO_LEASE, Leases.NO_LEASES, encodeLogIndex(42));
            entryWriter.writeChunkEndEntry(version, 17, 13);
            entryWriter.writeChunkStartEntry(
                    version, 11, 13, 4, 15, UNKNOWN_TX_SEQUENCE_NUMBER, NO_LEASE, Leases.NO_LEASES, encodeLogIndex(43));
            entryWriter.writeCommitEntry(version, 7, 8);
        }

        VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader(
                StorageEngineFactory.defaultStorageEngine().commandReaderFactory(), BINARY_VERSIONS, INSTANCE);
        try (var readChannel = ReadAheadUtils.newChannel(
                new PhysicalLogVersionedStoreChannel(
                        fs.read(path), 1, logFormat, path, EMPTY_ACCESSOR, DatabaseTracer.NULL),
                NO_MORE_CHANNELS,
                logHeader,
                INSTANCE)) {
            readChannel.position(logHeader.getStartPosition().getByteOffset());
            var startEntry = entryReader.readLogEntry(readChannel);
            var chunkEnd = entryReader.readLogEntry(readChannel);
            var chunkStart = entryReader.readLogEntry(readChannel);
            var commitEnd = entryReader.readLogEntry(readChannel);

            assertThat(startEntry).isInstanceOf(LogEntryStart.class);
            assertThat(commitEnd).isInstanceOf(LogEntryCommit.class);

            assertThat(((LogEntryStart) startEntry).getAdditionalHeader()).isEqualTo(encodeLogIndex(42));

            assertThat(chunkEnd).isInstanceOf(LogEntryChunkEnd.class);
            var chunkEndEntry = (LogEntryChunkEnd) chunkEnd;
            assertEquals(17, chunkEndEntry.getTransactionId());
            assertEquals(13, chunkEndEntry.getChunkId());

            assertThat(chunkStart).isInstanceOf(LogEntryChunkStart.class);
            var chunkStartEntry = (LogEntryChunkStart) chunkStart;
            assertEquals(11, chunkStartEntry.getTimeWritten());
            assertEquals(13, chunkStartEntry.getChunkId());
            assertThat(chunkStartEntry.getAdditionalHeader()).isEqualTo(encodeLogIndex(43));
        }
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "4.2") // Oldest version we can write
    void parseStartEntry(KernelVersion version) throws IOException {
        // given
        final LogEntryStart start = newStartEntry(
                version, 1, 2, 3, UNKNOWN_TX_SEQUENCE_NUMBER, 4, NO_LEASE, Leases.NO_LEASES, new byte[] {4});
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.putLong(start.getTimeWritten());
        channel.putLong(start.getLastCommittedTxWhenTransactionStarted());
        if (start instanceof LogEntryStartV4_2 v42) {
            channel.putInt(v42.getPreviousChecksum());
        } else if (start instanceof LogEntryStartV5_20 v520) {
            channel.putInt(v520.getPreviousChecksum());
        }

        if (version.isAtLeast(KernelVersion.VERSION_APPEND_INDEX_INTRODUCED)) {
            channel.putLong(start.getAppendIndex());
        }
        channel.putInt(start.getAdditionalHeader().length);
        channel.put(start.getAdditionalHeader(), start.getAdditionalHeader().length);

        channel.getCurrentLogPosition(marker);

        // when
        LogEntrySerializer<LogEntry> parser =
                serializationSet(version, BINARY_VERSIONS).select(LogEntryTypeCodes.TX_START);
        LogEntry logEntry = parser.parse(version, channel, marker, commandReader, INSTANCE);

        // then
        assertEquals(start, logEntry);
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "4.2") // Oldest version we can write
    void parseCorruptedStartEntry(KernelVersion version) {
        final LogEntryStart start = newStartEntry(
                version, 1, 2, 3, UNKNOWN_TX_SEQUENCE_NUMBER, 4, NO_LEASE, Leases.NO_LEASES, new byte[] {4});
        try (final InMemoryClosableChannel channel = new InMemoryClosableChannel()) {
            channel.putLong(start.getTimeWritten());
            channel.putLong(start.getLastCommittedTxWhenTransactionStarted());
            if (start instanceof LogEntryStartV4_2 v42) {
                channel.putInt(v42.getPreviousChecksum());
            } else if (start instanceof LogEntryStartV5_20 v520) {
                channel.putInt(v520.getPreviousChecksum());
            }
            if (version.isAtLeast(KernelVersion.VERSION_APPEND_INDEX_INTRODUCED)) {
                channel.putLong(start.getAppendIndex());
            }
            channel.putInt(Integer.MAX_VALUE);

            channel.getCurrentLogPosition(marker);

            LogEntrySerializer<LogEntry> parser =
                    serializationSet(version, BINARY_VERSIONS).select(LogEntryTypeCodes.TX_START);

            assertThatThrownBy(() -> parser.parse(version, channel, marker, commandReader, INSTANCE))
                    .isInstanceOf(IOException.class);
        }
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "4.2") // Oldest version we can write
    void parseCommitEntry(KernelVersion version) throws IOException {
        // given
        var expectedChecksums = new HashMap<KernelVersion, Integer>();
        expectedChecksums.put(KernelVersion.V4_2, -852558083);
        expectedChecksums.put(KernelVersion.V4_3_D4, -1832246622);
        expectedChecksums.put(KernelVersion.V4_4, 144196046);
        expectedChecksums.put(KernelVersion.V5_0, 1467784593);
        expectedChecksums.put(KernelVersion.V5_7, -1219364496);
        expectedChecksums.put(KernelVersion.V5_8, -390781649);
        expectedChecksums.put(KernelVersion.V5_9, -2045163175);
        expectedChecksums.put(KernelVersion.V5_10, -637692666);
        expectedChecksums.put(KernelVersion.V5_11, 969994727);
        expectedChecksums.put(KernelVersion.V5_12, 1714695608);
        expectedChecksums.put(KernelVersion.V5_13, -60404012);
        expectedChecksums.put(KernelVersion.V5_14, -1551723893);
        expectedChecksums.put(KernelVersion.V5_15, 1135605354);
        expectedChecksums.put(KernelVersion.V5_18, 474688053);
        expectedChecksums.put(KernelVersion.V5_19, -1627967866);
        expectedChecksums.put(KernelVersion.V5_20, -1055657255);
        expectedChecksums.put(KernelVersion.V5_22, 557749816);
        expectedChecksums.put(KernelVersion.V5_23, 2128235111);
        expectedChecksums.put(KernelVersion.V5_25, -460838645);
        expectedChecksums.put(KernelVersion.V2025_04, -1150487212);
        expectedChecksums.put(KernelVersion.V2025_05, 1531023797);
        expectedChecksums.put(KernelVersion.V2025_07, 77942250);
        expectedChecksums.put(KernelVersion.V2025_08, 1778983324);
        expectedChecksums.put(KernelVersion.V2025_09, 904789443);
        expectedChecksums.put(KernelVersion.V2025_10, -708798174);
        expectedChecksums.put(KernelVersion.V2025_11, -1977334403);
        expectedChecksums.put(KernelVersion.V2026_01, 276178449);
        expectedChecksums.put(KernelVersion.V2026_02, 1335032398);

        final LogEntryCommit commit = newCommitEntry(version, 42, 21, expectedChecksums.get(version));
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.beginChecksumForWriting();
        channel.putVersion(version.version());
        channel.putLong(commit.getTxId());
        channel.putLong(commit.getTimeWritten());
        channel.putChecksum();

        channel.getCurrentLogPosition(marker);

        // when
        byte readVersion = channel.getVersion();
        LogEntrySerializer<LogEntry> parser =
                serializationSet(version, BINARY_VERSIONS).select(LogEntryTypeCodes.TX_COMMIT);
        LogEntry logEntry = parser.parse(version, channel, marker, commandReader, INSTANCE);

        // then
        assertEquals(version.version(), readVersion);
        assertEquals(commit, logEntry);
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "4.2") // Oldest version we can write
    void shouldParserCommandsUsingAGivenFactory(KernelVersion version) throws IOException {
        // given
        // The record, it will be used as before and after
        TestCommand testCommand = new TestCommand(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9});
        final LogEntryCommand command = new LogEntryCommand(testCommand);
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();
        testCommand.serialize(channel);
        channel.getCurrentLogPosition(marker);

        // when
        LogEntrySerializer<LogEntry> parser =
                serializationSet(version, BINARY_VERSIONS).select(LogEntryTypeCodes.COMMAND);
        LogEntry logEntry = parser.parse(version, channel, marker, commandReader, INSTANCE);

        // then
        assertEquals(command, logEntry);
    }

    @ParameterizedTest
    @EnumSource
    void shouldThrowWhenParsingUnknownEntry(KernelVersion version) {
        assertThrows(IllegalArgumentException.class, () -> serializationSet(version, BINARY_VERSIONS)
                .select((byte) 42)); // unused, at lest for now
    }
}
