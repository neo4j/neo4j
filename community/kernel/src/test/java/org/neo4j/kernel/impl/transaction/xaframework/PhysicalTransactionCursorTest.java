/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import org.neo4j.kernel.impl.nioneo.xa.command.Command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PhysicalTransactionCursorTest
{

    private final ReadableLogChannel channel = mock( ReadableLogChannel.class );
    private final LogEntryReader<ReadableLogChannel> entryReader = mock( LogEntryReader.class );

    private static final LogEntry NULL_ENTRY = null;
    private static final LogEntry.Start A_START_ENTRY = new LogEntry.Start( 0, 0, 0l, 0l, null, LogPosition.UNSPECIFIED );
    private static final LogEntry.Commit A_COMMIT_ENTRY = new LogEntry.OnePhaseCommit( 42, 0 );
    private static final LogEntry.Command A_COMMAND_ENTRY = new LogEntry.Command( new Command.NodeCommand() );

    @Test
    public void shouldCloseTheUnderlyingChannel() throws IOException
    {
        // given
        final PhysicalTransactionCursor cursor = new PhysicalTransactionCursor( channel, entryReader );

        // when
        cursor.close();

        // then
        verify( channel, times( 1 ) ).close();
    }

    @Test
    public void shouldReturnFalseWhenThereAreNoEntries() throws IOException
    {
        // given
        final PhysicalTransactionCursor cursor = new PhysicalTransactionCursor( channel, entryReader );

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
        final PhysicalTransactionCursor cursor = new PhysicalTransactionCursor( channel, entryReader );

        when( entryReader.readLogEntry( channel ) ).thenReturn( A_START_ENTRY, NULL_ENTRY);

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
        final PhysicalTransactionCursor cursor = new PhysicalTransactionCursor( channel, entryReader );


        when( entryReader.readLogEntry( channel ) ).thenReturn( A_START_ENTRY, A_COMMAND_ENTRY, A_COMMIT_ENTRY);

        // when
        cursor.next();

        // then
        assertEquals(
                new CommittedTransactionRepresentation(
                        A_START_ENTRY,
                        new PhysicalTransactionRepresentation( Arrays.asList( A_COMMAND_ENTRY.getXaCommand()
                        ) ),
                        A_COMMIT_ENTRY
                ), cursor.get() );
    }
}
