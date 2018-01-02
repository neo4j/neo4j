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

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.function.Function;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.IdentifiableLogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogFilenames.getLegacyLogFilename;
import static org.neo4j.kernel.impl.transaction.log.LogPosition.UNSPECIFIED;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart.EMPTY_ADDITIONAL_ARRAY;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

public class LegacyLogEntryReaderTest
{
    private final FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
    private final File input = new File( getLegacyLogFilename( 3 ) );

    @Before
    public void setup() throws IOException
    {
        writeLogHeader( fs, input, 3l, -1 );
    }

    @Test
    public void shouldReadTheLogHeaderAndSetCurrentVersionAndABaseTxIdIfNegative() throws IOException
    {
        // given
        final LegacyLogEntryReader reader = new LegacyLogEntryReader( fs );

        // when
        final Pair<LogHeader, IOCursor<LogEntry>> pair = reader.openReadableChannel( input );

        // then
        pair.other().close(); // not null cursor
        assertEquals( new LogHeader( CURRENT_LOG_VERSION, 3l, BASE_TX_ID ), pair.first() );
    }

    @Test
    public void shouldReturnNoEntriesWhenTheChannelContainsNothing() throws IOException
    {
        // given
        final LogEntryReader<ReadableVersionableLogChannel> logEntryReader = mock( LogEntryReader.class );
        when( logEntryReader.readLogEntry( any( ReadableVersionableLogChannel.class ) ) ).thenReturn( null );
        final LegacyLogEntryReader reader = new LegacyLogEntryReader( fs,
                new Function<LogHeader,LogEntryReader<ReadableVersionableLogChannel>>()
        {
            @Override
            public LogEntryReader<ReadableVersionableLogChannel> apply( LogHeader from ) throws RuntimeException
            {
                return logEntryReader;
            }
        } );

        // when
        final IOCursor<LogEntry> cursor = reader.openReadableChannel( input ).other();

        // then
        assertFalse( cursor.next() );
        assertNull( cursor.get() );

        cursor.close();
    }

    @Test
    public void shouldReadTheEntries() throws IOException
    {
        // given
        final LogEntry start = new LogEntryStart( 0, 1, 2, 3, EMPTY_ADDITIONAL_ARRAY, UNSPECIFIED );
        final LogEntry command = new LogEntryCommand( new Command.NodeCommand() );
        final LogEntry commit = new OnePhaseCommit( 42, 43 );

        final LogEntryReader<ReadableVersionableLogChannel> logEntryReader = mock( LogEntryReader.class );
        when( logEntryReader.readLogEntry( any( ReadableVersionableLogChannel.class ) ) ).thenReturn(
                new IdentifiableLogEntry( start, 1 ),
                new IdentifiableLogEntry( command, 1 ),
                new IdentifiableLogEntry( commit, 1 ),
                null
        );

        final LegacyLogEntryReader reader = new LegacyLogEntryReader( fs,
                new Function<LogHeader,LogEntryReader<ReadableVersionableLogChannel>>()
        {
            @Override
            public LogEntryReader<ReadableVersionableLogChannel> apply( LogHeader from ) throws RuntimeException
            {
                return logEntryReader;
            }
        } );

        // when
        final IOCursor<LogEntry> cursor = reader.openReadableChannel( input ).other();

        // then
        assertTrue( cursor.next() );
        assertEquals( start, cursor.get() );
        assertTrue( cursor.next() );
        assertEquals( command, cursor.get() );
        assertTrue( cursor.next() );
        assertEquals( commit, cursor.get() );
        assertFalse( cursor.next() );

        cursor.close();
    }
}
