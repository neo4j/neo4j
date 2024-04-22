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
package org.neo4j.kernel.impl.transaction;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.LogIndexEncoding.encodeLogIndex;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newCommitEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newStartEntry;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.transaction.log.CommittedCommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkEnd;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkStart;

class CommittedCommandBatchCursorTest {
    private final ReadableLogChannel channel = mock(ReadableLogChannel.class, RETURNS_MOCKS);
    private final LogEntryReader entryReader = mock(LogEntryReader.class);

    private static final LogEntry NULL_ENTRY = null;
    private static final LogEntryStart START_ENTRY =
            newStartEntry(LATEST_KERNEL_VERSION, 0L, 0L, 0, 5, encodeLogIndex(2), LogPosition.UNSPECIFIED);
    private static final LogEntryCommit COMMIT_ENTRY = newCommitEntry(LATEST_KERNEL_VERSION, 42, 0, BASE_TX_CHECKSUM);
    private static final LogEntryCommand COMMAND_ENTRY = new LogEntryCommand(new TestCommand());
    private static final LogEntryChunkStart CHUNK_START =
            new LogEntryChunkStart(LATEST_KERNEL_VERSION, 12, 2, LogPosition.UNSPECIFIED);
    private static final LogEntryChunkEnd CHUNK_END =
            new LogEntryChunkEnd(LATEST_KERNEL_VERSION, 12, 2, BASE_TX_CHECKSUM);
    private CommittedCommandBatchCursor cursor;

    @BeforeEach
    void setup() throws IOException {
        cursor = new CommittedCommandBatchCursor(channel, entryReader);
    }

    @Test
    void readChunkFromLogs() throws IOException {
        when(entryReader.readLogEntry(channel)).thenReturn(START_ENTRY, CHUNK_END, CHUNK_START, COMMIT_ENTRY, null);

        assertTrue(cursor.next());
        assertThat(cursor.get()).isInstanceOf(CommittedChunkRepresentation.class);
        assertTrue(cursor.next());
        assertThat(cursor.get()).isInstanceOf(CommittedChunkRepresentation.class);

        assertFalse(cursor.next());
    }

    @Test
    void shouldCloseTheUnderlyingChannel() throws IOException {
        // when
        cursor.close();

        // then
        verify(channel).close();
    }

    @Test
    void shouldReturnFalseWhenThereAreNoEntries() throws IOException {
        // given
        when(entryReader.readLogEntry(channel)).thenReturn(NULL_ENTRY);

        // when
        final boolean result = cursor.next();

        // then
        assertFalse(result);
        assertNull(cursor.get());
    }

    @Test
    void shouldReturnFalseWhenThereIsAStartEntryButNoCommitEntries() throws IOException {
        // given
        when(entryReader.readLogEntry(channel)).thenReturn(START_ENTRY, NULL_ENTRY);

        // when
        final boolean result = cursor.next();

        // then
        assertFalse(result);
        assertNull(cursor.get());
    }

    @Test
    void shouldCallTheVisitorWithTheFoundTransaction() throws IOException {
        // given
        when(entryReader.readLogEntry(channel)).thenReturn(START_ENTRY, COMMAND_ENTRY, COMMIT_ENTRY);

        // when
        cursor.next();

        // then
        assertEquals(
                new CommittedTransactionRepresentation(
                        START_ENTRY, singletonList(COMMAND_ENTRY.getCommand()), COMMIT_ENTRY),
                cursor.get());
    }
}
