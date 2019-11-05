/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

class PhysicalTransactionCursorTest
{
    private final ReadableLogChannel channel = mock( ReadableLogChannel.class, RETURNS_MOCKS );
    private final LogEntryReader entryReader = mock( LogEntryReader.class );

    private static final LogEntry NULL_ENTRY = null;
    private static final CheckPoint A_CHECK_POINT_ENTRY = new CheckPoint( LogPosition.UNSPECIFIED );
    private static final LogEntryStart A_START_ENTRY = new LogEntryStart( 0L, 0L, 0, null, LogPosition.UNSPECIFIED );
    private static final LogEntryCommit A_COMMIT_ENTRY = new LogEntryCommit( 42, 0, BASE_TX_CHECKSUM );
    private static final LogEntryCommand A_COMMAND_ENTRY = new LogEntryCommand( new TestCommand() );
    private PhysicalTransactionCursor cursor;

    @BeforeEach
    void setup() throws IOException
    {
        cursor = new PhysicalTransactionCursor( channel, entryReader );
    }

    @Test
    void shouldCloseTheUnderlyingChannel() throws IOException
    {
        // when
        cursor.close();

        // then
        verify( channel ).close();
    }

    @Test
    void shouldReturnFalseWhenThereAreNoEntries() throws IOException
    {
        // given
        when( entryReader.readLogEntry( channel ) ).thenReturn( NULL_ENTRY );

        // when
        final boolean result = cursor.next();

        // then
        assertFalse( result );
        assertNull( cursor.get() );
    }

    @Test
    void shouldReturnFalseWhenThereIsAStartEntryButNoCommitEntries() throws IOException
    {
        // given
        when( entryReader.readLogEntry( channel ) ).thenReturn( A_START_ENTRY, NULL_ENTRY );

        // when
        final boolean result = cursor.next();

        // then
        assertFalse( result );
        assertNull( cursor.get() );
    }

    @Test
    void shouldCallTheVisitorWithTheFoundTransaction() throws IOException
    {
        // given
        when( entryReader.readLogEntry( channel ) ).thenReturn( A_START_ENTRY, A_COMMAND_ENTRY, A_COMMIT_ENTRY );

        // when
        cursor.next();

        // then
        PhysicalTransactionRepresentation txRepresentation =
                new PhysicalTransactionRepresentation( singletonList( A_COMMAND_ENTRY.getCommand() ) );
        assertEquals(
                new CommittedTransactionRepresentation( A_START_ENTRY, txRepresentation, A_COMMIT_ENTRY ),
                cursor.get()
        );
    }

    @Test
    void shouldSkipCheckPoints() throws IOException
    {
        // given
        when( entryReader.readLogEntry( channel ) ).thenReturn(
                A_CHECK_POINT_ENTRY,
                A_START_ENTRY, A_COMMAND_ENTRY, A_COMMIT_ENTRY,
                A_CHECK_POINT_ENTRY );

        // when
        cursor.next();

        // then
        PhysicalTransactionRepresentation txRepresentation =
                new PhysicalTransactionRepresentation( singletonList( A_COMMAND_ENTRY.getCommand() ) );
        assertEquals(
                new CommittedTransactionRepresentation( A_START_ENTRY, txRepresentation, A_COMMIT_ENTRY ),
                cursor.get()
        );
    }
}
