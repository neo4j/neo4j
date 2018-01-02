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
package org.neo4j.kernel.impl.storemigration.legacylogs;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.IdentifiableLogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.transaction.log.LogPosition.UNSPECIFIED;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart.EMPTY_ADDITIONAL_ARRAY;

public class LogEntrySortingCursorTest
{
    private static final Random random = new Random( 42l );
    private final ReadableVersionableLogChannel channel = mock( ReadableVersionableLogChannel.class );
    @SuppressWarnings("unchecked")
    private final LogEntryReader<ReadableVersionableLogChannel> reader = mock( LogEntryReader.class );

    @Test
    public void shouldDoNothingIfTheListIsOrdered() throws IOException
    {
        // given
        final LogEntry start1 = start( 1 );
        final LogEntry command1 = command();
        final LogEntry commit1 = commit( 2 );

        final LogEntry start2 = start( 2 );
        final LogEntry command2 = command();
        final LogEntry commit2 = commit( 3 );

        when( reader.readLogEntry( channel ) ).thenReturn(
                id( start1, 1 ), id( command1, 1 ), id( commit1, 1 ),
                id( start2, 2 ), id( command2, 2 ), id( commit2, 2 ),
                null
        );

        // when
        final LogEntrySortingCursor cursor = new LogEntrySortingCursor( reader, channel );

        // then
        final List<LogEntry> expected = Arrays.asList( start1, command1, commit1, start2, command2, commit2 );
        assertCursorContains( expected, cursor );
    }

    @Test
    public void shouldReorderBasedOnTheTxId() throws IOException
    {
        // given
        final LogEntry start1 = start( 3 );
        final LogEntry command1 = command();
        final LogEntry commit1 = commit( 5 );

        final LogEntry start2 = start( 3 );
        final LogEntry command2 = command();
        final LogEntry commit2 = commit( 4 );

        when( reader.readLogEntry( channel ) ).thenReturn(
                id( start1, 1 ), id( command1, 1 ),
                id( start2, 2 ), id( command2, 2 ), id( commit2, 2 ),
                id( commit1, 1 ),
                null
        );

        // when
        final LogEntrySortingCursor cursor = new LogEntrySortingCursor( reader, channel );

        // then
        final List<LogEntry> expected = Arrays.asList( start2, command2, commit2, start1, command1, commit1 );
        assertCursorContains( expected, cursor );
    }

    @Test
    public void shouldReorderWhenEntriesAreMixedUp() throws IOException
    {
        // given
        final LogEntry start1 = start( 3 );
        final LogEntry command1 = command();
        final LogEntry commit1 = commit( 5 );

        final LogEntry start2 = start( 3 );
        final LogEntry command2 = command();
        final LogEntry commit2 = commit( 4 );

        when( reader.readLogEntry( channel ) ).thenReturn(
                id( start2, 2 ),
                id( start1, 1 ),
                id( command1, 1 ),
                id( command2, 2 ),
                id( commit2, 2 ),
                id( commit1, 1 ),
                null
        );

        // when
        final LogEntrySortingCursor cursor = new LogEntrySortingCursor( reader, channel );

        // then
        final List<LogEntry> expected = Arrays.asList( start2, command2, commit2, start1, command1, commit1 );
        assertCursorContains( expected, cursor );
    }

    @Test
    public void shouldBeFineIfThereAreEntriesWithoutACommit() throws IOException
    {
        // given
        final LogEntry start1 = start( 3 );
        final LogEntry command1 = command();

        final LogEntry start2 = start( 3 );
        final LogEntry command2 = command();
        final LogEntry commit2 = commit( 4 );

        when( reader.readLogEntry( channel ) ).thenReturn(
                id( start2, 2 ),
                id( start1, 1 ),
                id( command1, 1 ),
                id( command2, 2 ),
                id( commit2, 2 ),
                null
        );

        // when
        final LogEntrySortingCursor cursor = new LogEntrySortingCursor( reader, channel );

        // then
        final List<LogEntry> expected = Arrays.asList( start2, command2, commit2 );
        assertCursorContains( expected, cursor );
    }

    private void assertCursorContains( Iterable<LogEntry> entries, LogEntrySortingCursor cursor ) throws IOException
    {
        for ( LogEntry entry : entries )
        {
            assertTrue( cursor.next() );
            assertEquals( entry, cursor.get() );
        }
        assertFalse( cursor.next() );
    }

    private LogEntry start( long lastTxId )
    {
        return new LogEntryStart( random.nextInt(), random.nextInt(), random.nextLong(),
                lastTxId, EMPTY_ADDITIONAL_ARRAY, UNSPECIFIED );
    }

    private LogEntry command()
    {
        final Command.NodeCommand command = new Command.NodeCommand();
        command.init( new NodeRecord( random.nextInt() ), new NodeRecord( random.nextInt() ) );
        return new LogEntryCommand( command );
    }

    private LogEntry commit( long txId )
    {
        return new OnePhaseCommit( txId, random.nextInt() );
    }

    private IdentifiableLogEntry id( LogEntry entry, int id )
    {
        return new IdentifiableLogEntry( entry, id );
    }
}
