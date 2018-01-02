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

import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.storemigration.FileOperation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.ArrayIOCursor;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogFilenames.getLegacyLogFilename;
import static org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogFilenames.versionedLegacyLogFilesFilter;
import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.DEFAULT_NAME;
import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.DEFAULT_VERSION_SUFFIX;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

public class LegacyLogsTest
{
    private static final long NO_NEXT_REL = Record.NO_NEXT_RELATIONSHIP.intValue();

    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final LegacyLogEntryReader reader = mock( LegacyLogEntryReader.class );
    private final LegacyLogEntryWriter writer = mock( LegacyLogEntryWriter.class );
    private final File storeDir = new File( "/store" );
    private final File migrationDir = new File( "/migration" );

    @Test
    public void shouldRewriteLogFiles() throws IOException
    {
        // given
        final IOCursor<LogEntry> cursor = mock( IOCursor.class );
        final LogVersionedStoreChannel writeChannel = mock( LogVersionedStoreChannel.class );
        final LogHeader header = new LogHeader( CURRENT_LOG_VERSION, 1, 42 );
        when( fs.listFiles( storeDir, versionedLegacyLogFilesFilter ) ).thenReturn( new File[]{
                new File( getLegacyLogFilename( 1 ) )
        } );
        when( reader.openReadableChannel( new File( getLegacyLogFilename( 1 ) ) ) ).thenReturn(
                Pair.of( header, cursor )
        );
        when( writer.openWritableChannel( new File( migrationDir, getLegacyLogFilename( 1 ) ) ) ).
                thenReturn( writeChannel );

        // when
        new LegacyLogs( fs, reader, writer ).migrateLogs( storeDir, migrationDir );

        // then
        verify( writer, times( 1 ) ).writeLogHeader( writeChannel, header );
        verify( writer, times( 1 ) ).writeAllLogEntries( writeChannel, cursor );
    }

    @Test
    public void shouldMoveFiles() throws IOException
    {
        // given
        final EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        fs.mkdirs( storeDir );
        fs.mkdirs( migrationDir );

        final Set<File> logsInStoreDir = new HashSet<>( Arrays.asList(
                new File( storeDir, getLegacyLogFilename( 1 ) ),
                new File( storeDir, getLegacyLogFilename( 2 ) )
        ) );

        final List<File> logsInMigrationDir = Arrays.asList(
                new File( migrationDir, getLegacyLogFilename( 1 ) ),
                new File( migrationDir, getLegacyLogFilename( 2 ) )
        );

        for ( File file : logsInMigrationDir )
        {
            try ( StoreChannel channel = fs.create( file ) )
            {
                ByteBuffer buffer = ByteBuffer.allocate( 8 );
                buffer.putLong( 42 );
                buffer.flip();
                channel.write( buffer );
            }
        }

        // should override older files
        for ( File file : logsInStoreDir )
        {
            try ( StoreChannel channel = fs.create( file ) )
            {
                ByteBuffer buffer = ByteBuffer.allocate( 8 );
                buffer.putLong( 13 );
                buffer.flip();
                channel.write( buffer );
            }
        }

        // when
        new LegacyLogs( fs, reader, writer ).operate( FileOperation.MOVE, migrationDir, storeDir );

        // then
        assertEquals( logsInStoreDir, new HashSet<>( Arrays.asList( fs.listFiles( storeDir ) ) ) );
        for ( File file : logsInStoreDir )
        {
            try ( StoreChannel channel = fs.open( file, "r" ) )
            {
                ByteBuffer buffer = ByteBuffer.allocate( 8 );
                channel.read( buffer );
                buffer.flip();
                assertEquals( 42, buffer.getLong() );
            }
        }
    }

    @Test
    public void shouldRenameFiles() throws IOException
    {
        // given
        final EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        fs.mkdirs( storeDir );
        final File unrelated = new File( storeDir, "unrelated" );
        final List<File> files = Arrays.asList(
                new File( storeDir, "active_tx_log" ),
                new File( storeDir, "tm_tx_log.v0" ),
                new File( storeDir, "tm_tx_log.v1" ),
                new File( storeDir, "nioneo_logical.log.1" ),
                new File( storeDir, "nioneo_logical.log.2" ),
                new File( storeDir, getLegacyLogFilename( 1 ) ),
                new File( storeDir, getLegacyLogFilename( 2 ) ),
                unrelated
        );

        for ( File file : files )
        {
            fs.create( file ).close();
        }

        // when
        new LegacyLogs( fs, reader, writer ).renameLogFiles( storeDir );

        // then
        final Set<File> expected = new HashSet<>( Arrays.asList(
                unrelated,
                new File( storeDir, getLogFilenameForVersion( 1 ) ),
                new File( storeDir, getLogFilenameForVersion( 2 ) )
        ) );
        assertEquals( expected, new HashSet<>( Arrays.asList( fs.listFiles( storeDir ) ) ) );
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void transactionInformationRetrievedFromCommitEntries() throws IOException
    {
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        File logFile = new File( LegacyLogFilenames.getLegacyLogFilename( 1 ) );
        when( fs.listFiles( any( File.class ), any( FilenameFilter.class ) ) )
                .thenReturn( new File[]{logFile} );

        LegacyLogEntryReader reader = mock( LegacyLogEntryReader.class );
        LogEntry[] entries = new LogEntry[]{
                start( 1 ), createNode( 1 ), createNode( 2 ), commit( 1 ),
                start( 2 ), createNode( 3 ), createNode( 4 ), commit( 2 ),
                start( 3 ), createNode( 5 ), commit( 3 )
        };
        when( reader.openReadableChannel( any( File.class ) ) )
                .thenReturn( readableChannel( entries ), readableChannel( entries ), readableChannel( entries ) );

        LegacyLogEntryWriter writer = new LegacyLogEntryWriter( fs );
        LegacyLogs legacyLogs = new LegacyLogs( fs, reader, writer );

        assertEquals( newTransactionId( 1 ), legacyLogs.getTransactionInformation( storeDir, 1 ) );
        assertEquals( newTransactionId( 2 ), legacyLogs.getTransactionInformation( storeDir, 2 ) );
        assertEquals( newTransactionId( 3 ), legacyLogs.getTransactionInformation( storeDir, 3 ) );
    }

    private String getLogFilenameForVersion( int version )
    {
        return DEFAULT_NAME + DEFAULT_VERSION_SUFFIX + version;
    }

    private static TransactionId newTransactionId( int id )
    {
        return new TransactionId( id, LogEntryStart.checksum( new byte[]{(byte) id}, id, id ), id );
    }

    private static LogEntry start( int id )
    {
        return new LogEntryStart( id, id, 1, id - 1, new byte[]{(byte) id}, new LogPosition( 1, 1 ) );
    }

    private static LogEntry createNode( int id )
    {
        Command.NodeCommand command = new Command.NodeCommand();
        NodeRecord before = new NodeRecord( id, false, NO_NEXT_REL, NO_NEXT_REL );
        NodeRecord after = new NodeRecord( id, true, NO_NEXT_REL, NO_NEXT_REL );
        command.init( before, after );
        return new LogEntryCommand( command );
    }

    private static LogEntry commit( int id )
    {
        return new OnePhaseCommit( id, id );
    }

    private Pair<LogHeader,IOCursor<LogEntry>> readableChannel( LogEntry[] entries )
    {
        IOCursor<LogEntry> cursor = new ArrayIOCursor<>( entries );
        LogHeader logHeader = new LogHeader( (byte) 1, 1, 1 );
        return Pair.of( logHeader, cursor );
    }
}
