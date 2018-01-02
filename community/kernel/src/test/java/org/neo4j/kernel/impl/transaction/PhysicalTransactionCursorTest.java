/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PhysicalTransactionCursorTest
{
    private final ReadableVersionableLogChannel channel = mock( ReadableVersionableLogChannel.class, RETURNS_MOCKS );
    private final LogEntryReader<ReadableVersionableLogChannel> entryReader = mock( LogEntryReader.class );

    private static final LogEntry NULL_ENTRY = null;
    private static final CheckPoint A_CHECK_POINT_ENTRY = new CheckPoint( LogPosition.UNSPECIFIED );
    private static final LogEntryStart A_START_ENTRY = new LogEntryStart( 0, 0, 0l, 0l, null, LogPosition.UNSPECIFIED );
    private static final LogEntryCommit A_COMMIT_ENTRY = new OnePhaseCommit( 42, 0 );
    private static final LogEntryCommand A_COMMAND_ENTRY = new LogEntryCommand( new Command.NodeCommand() );
    private PhysicalTransactionCursor<ReadableVersionableLogChannel> cursor;

    @Before
    public void setup() throws IOException
    {
        cursor = new PhysicalTransactionCursor<>( channel, entryReader );
    }

    @Test
    public void shouldCloseTheUnderlyingChannel() throws IOException
    {
        // when
        cursor.close();

        // then
        verify( channel, times( 1 ) ).close();
    }

    @Test
    public void shouldReturnFalseWhenThereAreNoEntries() throws IOException
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
    public void shouldReturnFalseWhenThereIsAStartEntryButNoCommitEntries() throws IOException
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
    public void shouldCallTheVisitorWithTheFoundTransaction() throws IOException
    {
        // given
        when( entryReader.readLogEntry( channel ) ).thenReturn( A_START_ENTRY, A_COMMAND_ENTRY, A_COMMIT_ENTRY );

        // when
        cursor.next();

        // then
        PhysicalTransactionRepresentation txRepresentation =
                new PhysicalTransactionRepresentation( singletonList( A_COMMAND_ENTRY.getXaCommand() ) );
        assertEquals(
                new CommittedTransactionRepresentation( A_START_ENTRY, txRepresentation, A_COMMIT_ENTRY ),
                cursor.get()
        );
    }

    @Test
    public void shouldSkipCheckPoints() throws IOException
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
                new PhysicalTransactionRepresentation( singletonList( A_COMMAND_ENTRY.getXaCommand() ) );
        assertEquals(
                new CommittedTransactionRepresentation( A_START_ENTRY, txRepresentation, A_COMMIT_ENTRY ),
                cursor.get()
        );
    }
}
