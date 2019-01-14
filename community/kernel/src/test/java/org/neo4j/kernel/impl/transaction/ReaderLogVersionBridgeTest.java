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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.encodeLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

public class ReaderLogVersionBridgeTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final LogVersionedStoreChannel channel = mock( LogVersionedStoreChannel.class );

    private final long version = 10L;
    private LogFiles logFiles;

    @Before
    public void setUp() throws Exception
    {
        logFiles = prepareLogFiles();
    }

    @Test
    public void shouldOpenTheNextChannelWhenItExists() throws IOException
    {
        // given
        final StoreChannel newStoreChannel = mock( StoreChannel.class );
        final ReaderLogVersionBridge bridge = new ReaderLogVersionBridge( logFiles );

        when( channel.getVersion() ).thenReturn( version );
        when( channel.getLogFormatVersion() ).thenReturn( CURRENT_LOG_VERSION );
        when( fs.fileExists( any( File.class ) ) ).thenReturn( true );
        when( fs.open( any( File.class ), eq( OpenMode.READ ) ) ).thenReturn( newStoreChannel );
        when( newStoreChannel.read( ArgumentMatchers.<ByteBuffer>any() ) ).then( invocationOnMock ->
        {
            ByteBuffer buffer = invocationOnMock.getArgument( 0 );
            buffer.putLong( encodeLogVersion( version + 1 ) );
            buffer.putLong( 42 );
            return LOG_HEADER_SIZE;
        } );

        // when
        final LogVersionedStoreChannel result = bridge.next( channel );

        // then
        PhysicalLogVersionedStoreChannel expected =
                new PhysicalLogVersionedStoreChannel( newStoreChannel, version + 1, CURRENT_LOG_VERSION );
        assertEquals( expected, result );
        verify( channel, times( 1 ) ).close();
    }

    @Test
    public void shouldReturnOldChannelWhenThereIsNoNextChannel() throws IOException
    {
        // given
        final ReaderLogVersionBridge bridge = new ReaderLogVersionBridge( logFiles );

        when( channel.getVersion() ).thenReturn( version );
        when( fs.open( any( File.class ), eq( OpenMode.READ ) ) ).thenThrow( new FileNotFoundException() );

        // when
        final LogVersionedStoreChannel result = bridge.next( channel );

        // then
        assertEquals( channel, result );
        verify( channel, never() ).close();
    }

    @Test
    public void shouldReturnOldChannelWhenNextChannelHasntGottenCompleteHeaderYet() throws Exception
    {
        // given
        final ReaderLogVersionBridge bridge = new ReaderLogVersionBridge( logFiles );
        final StoreChannel nextVersionWithIncompleteHeader = mock( StoreChannel.class );
        when( nextVersionWithIncompleteHeader.read( any( ByteBuffer.class ) ) ).thenReturn( LOG_HEADER_SIZE / 2 );

        when( channel.getVersion() ).thenReturn( version );
        when( fs.fileExists( any( File.class ) ) ).thenReturn( true );
        when( fs.open( any( File.class ), eq( OpenMode.READ ) ) ).thenReturn( nextVersionWithIncompleteHeader );

        // when
        final LogVersionedStoreChannel result = bridge.next( channel );

        // then
        assertEquals( channel, result );
        verify( channel, never() ).close();
    }

    private LogFiles prepareLogFiles() throws IOException
    {
        return LogFilesBuilder.logFilesBasedOnlyBuilder( testDirectory.directory(), fs ).build();
    }
}
