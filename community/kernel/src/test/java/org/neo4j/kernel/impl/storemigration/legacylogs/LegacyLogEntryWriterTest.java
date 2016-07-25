/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

import org.junit.Test;

import org.neo4j.cursor.IOCursor;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.ArrayIOCursor;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogFilenames.getLegacyLogFilename;
import static org.neo4j.kernel.impl.transaction.log.LogPosition.UNSPECIFIED;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart.EMPTY_ADDITIONAL_ARRAY;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

public class LegacyLogEntryWriterTest
{
    private final FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
    private final NodeRecord nodeRecord = new NodeRecord( 42L );
    private final RelationshipRecord relRecord = new RelationshipRecord( 42L );

    @Test
    public void shouldWriteTheHeaderInTheFile() throws IOException
    {
        // given
        final LegacyLogEntryWriter writer = new LegacyLogEntryWriter( fs );
        final File output = new File( getLegacyLogFilename( 3 ) );
        final LogHeader header = new LogHeader( CURRENT_LOG_VERSION, 1, 42L );

        // when
        try ( LogVersionedStoreChannel channel = writer.openWritableChannel( output ) )
        {
            writer.writeLogHeader( channel, header );
        }
        // then
        assertEquals( header, readLogHeader( fs, output ) );
    }

    @Test
    public void shouldWriteAllTheEntryInACommitToTheFile() throws IOException
    {
        // given
        final LogVersionedStoreChannel channel = mock( LogVersionedStoreChannel.class );
        final LogEntryWriter logEntryWriter = mock( LogEntryWriter.class );
        final LegacyLogEntryWriter writer = new LegacyLogEntryWriter( fs, liftToFactory( logEntryWriter ) );
        final LogEntryStart start = new LogEntryStart( 0, 1, 2L, 3L, EMPTY_ADDITIONAL_ARRAY, UNSPECIFIED );
        final LogEntryCommand command = new LogEntryCommand( new Command.NodeCommand( nodeRecord, nodeRecord ) );
        final LogEntryCommit commit = new OnePhaseCommit( 42L, 43L );

        // when
        final IOCursor<LogEntry> cursor = mockCursor( start, command, commit );
        writer.writeAllLogEntries( channel, cursor );

        // then
        verify( logEntryWriter, times( 1 ) ).writeStartEntry( 0, 1, 2L, 3L, EMPTY_ADDITIONAL_ARRAY );
        final TransactionRepresentation expected =
                new PhysicalTransactionRepresentation( Arrays.asList( command.getXaCommand() ) );
        verify( logEntryWriter, times( 1 ) ).serialize( eq( expected ) );
        verify( logEntryWriter, times( 1 ) ).writeCommitEntry( 42L, 43L );
    }

    @Test
    public void shouldWriteAllTheEntryInSeveralCommitsToTheFile() throws IOException
    {
        // given
        final LogVersionedStoreChannel channel = mock( LogVersionedStoreChannel.class );
        final LogEntryWriter logEntryWriter = mock( LogEntryWriter.class );
        final LegacyLogEntryWriter writer = new LegacyLogEntryWriter( fs, liftToFactory( logEntryWriter ) );
        final LogEntryStart start1 = new LogEntryStart( 0, 1, 2L, 3L, EMPTY_ADDITIONAL_ARRAY, UNSPECIFIED );
        final LogEntryCommand command1 = new LogEntryCommand( new Command.NodeCommand( nodeRecord, nodeRecord ) );
        final LogEntryCommit commit1 = new OnePhaseCommit( 42L, 43L );
        final LogEntryStart start2 = new LogEntryStart( 9, 8, 7L, 6L, EMPTY_ADDITIONAL_ARRAY, UNSPECIFIED );
        final LogEntryCommand command2 = new LogEntryCommand( new Command.RelationshipCommand( relRecord, relRecord ) );
        final LogEntryCommit commit2 = new OnePhaseCommit( 84L, 85L );

        // when
        IOCursor<LogEntry> cursor = mockCursor( start1, command1, commit1, start2, command2, commit2 );
        writer.writeAllLogEntries( channel, cursor );

        // then
        verify( logEntryWriter, times( 1 ) ).writeStartEntry( 0, 1, 2L, 3L, EMPTY_ADDITIONAL_ARRAY );
        final TransactionRepresentation expected1 =
                new PhysicalTransactionRepresentation( Arrays.asList( command1.getXaCommand() ) );
        verify( logEntryWriter, times( 1 ) ).serialize( eq( expected1 ) );
        verify( logEntryWriter, times( 1 ) ).writeCommitEntry( 42L, 43L );
        verify( logEntryWriter, times( 1 ) ).writeStartEntry( 9, 8, 7L, 6L, EMPTY_ADDITIONAL_ARRAY );
        final TransactionRepresentation expected2 =
                new PhysicalTransactionRepresentation( Arrays.asList( command2.getXaCommand() ) );
        verify( logEntryWriter, times( 1 ) ).serialize( eq( expected2 ) );
        verify( logEntryWriter, times( 1 ) ).writeCommitEntry( 84L, 85L );
    }

    private Function<FlushableChannel, LogEntryWriter> liftToFactory( final LogEntryWriter logEntryWriter )
    {
        return ignored -> logEntryWriter;
    }

    private IOCursor<LogEntry> mockCursor( final LogEntry... entries )
    {
        return new ArrayIOCursor<>( entries );
    }
}
