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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReaderLogVersionBridgeTest
{
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final PhysicalLogFiles logFiles = mock( PhysicalLogFiles.class );
    private final VersionedStoreChannel channel = mock( VersionedStoreChannel.class );

    private final File file = mock( File.class );
    private final long version = 10l;

    @Test
    public void shouldOpenTheNextChannelWhenItExists() throws IOException
    {
        // given
        final StoreChannel newStoreChannel = mock( StoreChannel.class );
        final ReaderLogVersionBridge bridge = new ReaderLogVersionBridge( fs, logFiles );

        when( channel.getVersion() ).thenReturn( version );
        when( logFiles.getVersionFileName( version + 1 ) ).thenReturn( file );
        when( fs.open( file, "r" ) ).thenReturn( newStoreChannel );

        // when
        final VersionedStoreChannel result = bridge.next( channel );

        // then
        assertEquals( new PhysicalLogVersionedStoreChannel( newStoreChannel, version + 1 ), result );
        verify( channel, times( 1 ) ).close();
    }

    @Test
    public void shouldReturnOldChannelWhenThereIsNoNextChannel() throws IOException
    {
        // given
        final ReaderLogVersionBridge bridge = new ReaderLogVersionBridge( fs, logFiles );

        when( channel.getVersion() ).thenReturn( version );
        when( logFiles.getVersionFileName( version + 1 ) ).thenReturn( file );
        when( fs.open( file, "r" ) ).thenThrow( new FileNotFoundException() );

        // when
        final VersionedStoreChannel result = bridge.next( channel );

        // then
        assertEquals( channel, result );
        verify( channel, never() ).close();
    }
}
