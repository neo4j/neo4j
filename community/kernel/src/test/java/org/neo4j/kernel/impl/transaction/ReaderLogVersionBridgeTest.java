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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;

import static org.junit.Assert.assertEquals;
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
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final PhysicalLogFiles logFiles = mock( PhysicalLogFiles.class );
    private final LogVersionedStoreChannel channel = mock( LogVersionedStoreChannel.class );

    private final File file = mock( File.class );
    private final long version = 10l;

    @Test
    public void shouldOpenTheNextChannelWhenItExists() throws IOException
    {
        // given
        final StoreChannel newStoreChannel = mock( StoreChannel.class );
        final ReaderLogVersionBridge bridge = new ReaderLogVersionBridge( fs, logFiles );

        when( channel.getVersion() ).thenReturn( version );
        when( channel.getLogFormatVersion() ).thenReturn( CURRENT_LOG_VERSION );
        when( logFiles.getLogFileForVersion( version + 1 ) ).thenReturn( file );
        when( fs.fileExists( file ) ).thenReturn( true );
        when( fs.open( file, "rw" ) ).thenReturn( newStoreChannel );
        when( newStoreChannel.read( Matchers.<ByteBuffer>any() ) ).then( new Answer<Integer>()
        {
            @Override
            public Integer answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                ByteBuffer buffer = (ByteBuffer) invocationOnMock.getArguments()[0];
                buffer.putLong( encodeLogVersion( version + 1 ) );
                buffer.putLong( 42 );
                return LOG_HEADER_SIZE;
            }
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
        final ReaderLogVersionBridge bridge = new ReaderLogVersionBridge( fs, logFiles );

        when( channel.getVersion() ).thenReturn( version );
        when( logFiles.getLogFileForVersion( version + 1 ) ).thenReturn( file );
        when( fs.open( file, "rw" ) ).thenThrow( new FileNotFoundException() );

        // when
        final LogVersionedStoreChannel result = bridge.next( channel );

        // then
        assertEquals( channel, result );
        verify( channel, never() ).close();
    }
}
