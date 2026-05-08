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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.kernel.KernelVersion.VERSION_APPEND_INDEX_INTRODUCED;
import static org.neo4j.kernel.KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED;
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newCommitEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newStartEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializationSets.serializationSet;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_SEQUENCE_NUMBER;
import static org.neo4j.test.LatestVersions.BINARY_VERSIONS;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.arguments.KernelVersionSource;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class VersionAwareLogEntryReaderTest {
    @Inject
    private RandomSupport random;

    private final LogEntryReader logEntryReader = logEntryReader(TestCommandReaderFactory.INSTANCE);

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void shouldReadAStartLogEntry(KernelVersion kernelVersion) throws IOException {
        // given
        final LogEntryStart start = newStartEntry(
                kernelVersion,
                1,
                2,
                3,
                UNKNOWN_TX_SEQUENCE_NUMBER,
                BASE_TX_CHECKSUM,
                NO_LEASE,
                Leases.NO_LEASES,
                new byte[] {4});
        final InMemoryClosableChannel channel = new InMemoryClosableChannel(true);

        writeEntry(channel, start, serializationSet(kernelVersion, BINARY_VERSIONS));

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry(channel);

        // then
        assertEquals(start, logEntry, additionalDebugInfo(kernelVersion, start));
        // <LogEntryStartV5_20[kernelVersion=KernelVersion{V5_0,version=5}
    }

    static String additionalDebugInfo(KernelVersion kernelVersion, LogEntry start) {
        final LogEntryStart recreatedStart = newStartEntry(
                kernelVersion,
                1,
                2,
                3,
                UNKNOWN_TX_SEQUENCE_NUMBER,
                BASE_TX_CHECKSUM,
                NO_LEASE,
                Leases.NO_LEASES,
                new byte[] {4});
        return String.format(
                """
Additional debug information:
kernelVersion = %s,
version.isAtLeast(VERSION_ENVELOPED_TRANSACTION_LOGS_INTRODUCED) = %s,
version.isAtLeast(VERSION_APPEND_INDEX_INTRODUCED) = %s,
start = %s,
recreatedStart = %s,
""",
                kernelVersion,
                kernelVersion.isAtLeast(VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED),
                kernelVersion.isAtLeast(VERSION_APPEND_INDEX_INTRODUCED),
                start,
                recreatedStart);
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void shouldReadACommitLogEntry(KernelVersion kernelVersion) throws IOException {
        // given
        EnumMap<KernelVersion, Integer> checksums = new EnumMap<>(KernelVersion.class);
        checksums.put(KernelVersion.V5_0, 1987705307);
        checksums.put(KernelVersion.V5_7, 1740832678);
        checksums.put(KernelVersion.V5_8, 1756102029);
        checksums.put(KernelVersion.V5_9, 1073537540);
        checksums.put(KernelVersion.V5_10, 815128623);
        checksums.put(KernelVersion.V5_11, 556721746);
        checksums.put(KernelVersion.V5_12, 776463481);
        checksums.put(KernelVersion.V5_13, 39381672);
        checksums.put(KernelVersion.V5_14, 221374595);
        checksums.put(KernelVersion.V5_15, 478867198);
        checksums.put(KernelVersion.V5_18, 333704405);
        checksums.put(KernelVersion.V5_19, -1306209812);
        checksums.put(KernelVersion.V5_20, -1118972985);
        checksums.put(KernelVersion.V5_22, -1393109574);
        checksums.put(KernelVersion.V5_23, -1549805679);
        checksums.put(KernelVersion.V5_25, -1887381184);
        checksums.put(KernelVersion.V2025_04, -2132157589);
        checksums.put(KernelVersion.V2025_05, -1856840426);
        checksums.put(KernelVersion.V2025_07, -1640243395);
        checksums.put(KernelVersion.V2025_08, -915619660);
        checksums.put(KernelVersion.V2025_09, -972701025);
        checksums.put(KernelVersion.V2025_10, -675363614);
        checksums.put(KernelVersion.V2025_11, -656947511);
        checksums.put(KernelVersion.V2026_01, -188168168);
        checksums.put(KernelVersion.V2026_02, -73283021);

        final LogEntryCommit commit = newCommitEntry(kernelVersion, 42, 21, checksums.get(kernelVersion));
        final InMemoryClosableChannel channel = new InMemoryClosableChannel(true);

        writeEntry(channel, commit, serializationSet(kernelVersion, BINARY_VERSIONS));

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry(channel);

        // then
        assertEquals(commit, logEntry);
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void shouldReadACommandLogEntry(KernelVersion kernelVersion) throws IOException {
        // given
        TestCommand testCommand = new TestCommand(new byte[] {100, 101, 102});
        final LogEntryCommand command = new LogEntryCommand(testCommand);
        final InMemoryClosableChannel channel = new InMemoryClosableChannel(true);

        writeCommand(kernelVersion, channel, testCommand);

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry(channel);

        // then
        assertEquals(command, logEntry);
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void shouldReturnNullWhenThereIsNoCommand(KernelVersion kernelVersion) throws IOException {
        // given
        final InMemoryClosableChannel channel = new InMemoryClosableChannel(true);

        channel.putVersion(kernelVersion.version());
        channel.put(LogEntryTypeCodes.COMMAND);
        channel.put(CommandReader.NONE);

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry(channel);

        // then
        assertNull(logEntry);
    }

    @Test
    void shouldReturnNullWhenNotEnoughDataInTheChannel() throws IOException {
        // given
        final InMemoryClosableChannel channel = new InMemoryClosableChannel(true);

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry(channel);

        // then
        assertNull(logEntry);
    }

    @Test
    void shouldContinueReadingThroughSkippedCommands() throws IOException {
        // given
        var channel = new InMemoryClosableChannel(true);
        int numCommands = random.nextInt(5, 10);
        int[] commandsToSkip = random.selection(IntStream.range(0, numCommands).toArray(), 1, numCommands - 1, false);
        List<LogEntry> expectedEntries = new ArrayList<>(numCommands);
        for (int i = 0; i < numCommands; i++) {
            var command = new TestCommand(random.nextBytes(random.nextInt(1, 10)));
            var entry = new LogEntryCommand(command);
            writeCommand(LATEST_KERNEL_VERSION, channel, command);
            if (!ArrayUtils.contains(commandsToSkip, i)) {
                expectedEntries.add(entry);
            }
        }

        // when
        var logEntryReader = logEntryReader(new SkippingCommandReaderFactory(commandsToSkip));
        List<LogEntry> readCommands = new ArrayList<>();
        LogEntry lastRead;
        while ((lastRead = logEntryReader.readLogEntry(channel)) != null) {
            readCommands.add(lastRead);
        }

        // then
        assertThat(readCommands).isEqualTo(expectedEntries);
    }

    private static void writeEntry(
            InMemoryClosableChannel channel, LogEntry entry, LogEntrySerializationSet serializationSet)
            throws IOException {
        serializationSet.select(entry.getType()).write(channel, entry);
    }

    private static VersionAwareLogEntryReader logEntryReader(CommandReaderFactory commandReaderFactory) {
        return new VersionAwareLogEntryReader(
                commandReaderFactory, LatestVersions.BINARY_VERSIONS, EmptyMemoryTracker.INSTANCE);
    }

    private static void writeCommand(
            KernelVersion kernelVersion, InMemoryClosableChannel channel, TestCommand testCommand) throws IOException {
        channel.putVersion(kernelVersion.version());
        channel.put(LogEntryTypeCodes.COMMAND);
        testCommand.serialize(channel);
    }

    private static class SkippingCommandReaderFactory implements CommandReaderFactory {
        private final int[] commandsToSkip;
        private int index;

        SkippingCommandReaderFactory(int[] commandsToSkip) {
            this.commandsToSkip = commandsToSkip;
        }

        @Override
        public CommandReader get(KernelVersion version) {
            var actual = TestCommandReaderFactory.INSTANCE.get(version);
            return new CommandReader() {
                @Override
                public KernelVersion kernelVersion() {
                    return actual.kernelVersion();
                }

                @Override
                public StorageCommand read(ReadableChannel channel, MemoryTracker memoryTracker) throws IOException {
                    var command = actual.read(channel, memoryTracker);
                    if (ArrayUtils.contains(commandsToSkip, index++)) {
                        return StorageCommand.SKIP;
                    }
                    return command;
                }
            };
        }
    }
}
