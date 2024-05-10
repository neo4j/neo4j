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
package org.neo4j.kernel.impl.transaction.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newCommitEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newStartEntry;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryChunkStartV5_20;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryRollbackV5_20;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkStart;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryRollback;

class AppendChunkPositionLocatorTest {

    private static final long TX_ID = 42;
    private static final long APPEND_INDEX = 43;
    private static final LogPosition BEFORE_START = new LogPosition(1L, LATEST_LOG_FORMAT.getHeaderSize());
    private static final LogPosition AFTER_COMMIT = new LogPosition(1L, 666L);

    private static final LogEntryStart START =
            newStartEntry(LATEST_KERNEL_VERSION, 0, 0, APPEND_INDEX, 1, null, BEFORE_START);
    private static final LogEntryChunkStart CHUNK_START =
            new LogEntryChunkStartV5_20(LATEST_KERNEL_VERSION, 0, 1, APPEND_INDEX, LogPosition.UNSPECIFIED);
    private static final LogEntryRollback ROLLBACK =
            new LogEntryRollbackV5_20(LATEST_KERNEL_VERSION, 0, APPEND_INDEX, 17, 7896);

    private static final LogEntryCommand COMMAND = new LogEntryCommand(new TestCommand());
    private static final LogEntryCommit COMMIT =
            newCommitEntry(LATEST_KERNEL_VERSION, TX_ID, System.currentTimeMillis(), BASE_TX_CHECKSUM);

    private final LogEntryReader logEntryReader = mock(LogEntryReader.class);
    private final ReadableLogPositionAwareChannel channel = mock(ReadableLogPositionAwareChannel.class);

    @Test
    void throwIfVisitNotCalledBeforeLogPosition() {
        final var locator = new AppendedChunkPositionLocator(APPEND_INDEX, logEntryReader);

        assertThatThrownBy(locator::getLogPositionOrThrow)
                .isInstanceOf(NoSuchLogEntryException.class)
                .hasMessage(
                        "Unable to find transaction or chunk with append index %d in any of available logical logs.",
                        APPEND_INDEX);
    }

    @Test
    void findLogPositionByStartAppendIndex() throws IOException {
        when(channel.getCurrentLogPosition()).thenReturn(BEFORE_START);
        when(logEntryReader.readLogEntry(channel)).thenReturn(START, COMMAND, COMMIT, null);

        final var locator = new AppendedChunkPositionLocator(APPEND_INDEX, logEntryReader);

        assertThat(locator.visit(channel)).isFalse();
        assertEquals(BEFORE_START, locator.getLogPositionOrThrow());
    }

    @Test
    void findLogPositionByChunkStartAppendIndex() throws IOException {
        when(channel.getCurrentLogPosition()).thenReturn(BEFORE_START);
        when(logEntryReader.readLogEntry(channel)).thenReturn(CHUNK_START, COMMAND, COMMIT, null);

        final var locator = new AppendedChunkPositionLocator(APPEND_INDEX, logEntryReader);

        assertThat(locator.visit(channel)).isFalse();
        assertEquals(BEFORE_START, locator.getLogPositionOrThrow());
    }

    @Test
    void findLogPositionByRollbackAppendIndex() throws IOException {
        when(channel.getCurrentLogPosition()).thenReturn(BEFORE_START);
        when(logEntryReader.readLogEntry(channel)).thenReturn(ROLLBACK, (LogEntry) null);

        final var locator = new AppendedChunkPositionLocator(APPEND_INDEX, logEntryReader);

        assertThat(locator.visit(channel)).isFalse();
        assertEquals(BEFORE_START, locator.getLogPositionOrThrow());
    }

    @Test
    void findChannelLogPositionIfAppendIndexNotFound() throws IOException {
        when(channel.getCurrentLogPosition()).thenReturn(BEFORE_START, BEFORE_START, BEFORE_START, AFTER_COMMIT);
        when(logEntryReader.readLogEntry(channel)).thenReturn(START, COMMAND, COMMIT, null);

        final var locator = new AppendedChunkPositionLocator(APPEND_INDEX + 1, logEntryReader);

        assertThat(locator.visit(channel)).isTrue();
        assertThat(locator.getLogPositionOrThrow()).isEqualTo(AFTER_COMMIT);
    }
}
