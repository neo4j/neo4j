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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newCommitEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newStartEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializationSets.serializationSet;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.BINARY_VERSIONS;

import java.io.IOException;
import java.util.EnumMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.arguments.KernelVersionSource;

class VersionAwareLogEntryReaderTest {
    private final LogEntryReader logEntryReader =
            new VersionAwareLogEntryReader(TestCommandReaderFactory.INSTANCE, LatestVersions.BINARY_VERSIONS);

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void shouldReadAStartLogEntry(KernelVersion kernelVersion) throws IOException {
        // given
        final LogEntryStart start =
                newStartEntry(kernelVersion, 1, 2, BASE_TX_CHECKSUM, new byte[] {4}, new LogPosition(0, 0));
        final InMemoryClosableChannel channel = new InMemoryClosableChannel(true);

        writeEntry(channel, start, serializationSet(kernelVersion, BINARY_VERSIONS));

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry(channel);

        // then
        assertEquals(start, logEntry);
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

        channel.put(kernelVersion.version());
        channel.put(LogEntryTypeCodes.COMMAND);
        testCommand.serialize(channel);

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

        channel.put(kernelVersion.version());
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

    private static void writeEntry(
            InMemoryClosableChannel channel, LogEntry start1, LogEntrySerializationSet serializationSet)
            throws IOException {
        serializationSet.select(start1.getType()).write(channel, start1);
    }
}
