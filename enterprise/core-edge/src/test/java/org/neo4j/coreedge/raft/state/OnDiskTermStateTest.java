/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.state;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.raft.state.term.OnDiskTermState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OnDiskTermStateTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    private OnDiskTermState createTermState( FileSystemAbstraction fileSystem ) throws IOException
    {
        final Supplier mock = mock( Supplier.class );
        when( mock.get() ).thenReturn( mock( DatabaseHealth.class ) );

        return new OnDiskTermState( fileSystem, testDir.directory(), 100, mock );
    }

    @Test
    public void shouldCallWriteAllAndForceOnVoteUpdate() throws Exception
    {
        // Given
        StoreFileChannel channel = mock( StoreFileChannel.class );
        when( channel.read( any( ByteBuffer.class ) ) ).thenReturn( -1 );
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
        when( fsa.open( any( File.class ), anyString() ) ).thenReturn( channel );

        OnDiskTermState state = new OnDiskTermState( fsa, testDir.directory(), 100, mock( Supplier.class ) );

        // When
        state.update( 100L );

        // Then
        verify( channel ).write( any( ByteBuffer.class ) );
        verify( channel ).flush();
    }

    @Test
    public void termShouldRemainUnchangedOnFailureToWriteToDisk() throws Exception
    {
        // Given
        StoreFileChannel channel = mock( StoreFileChannel.class );
        when( channel.read( any( ByteBuffer.class ) ) ).thenReturn( -1 );
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
        when( fsa.open( any( File.class ), anyString() ) ).thenReturn( channel );
        doThrow( new IOException() ).when( channel ).write( any( ByteBuffer.class ) );

        OnDiskTermState store = new OnDiskTermState( fsa, testDir.directory(), 100, mock( Supplier.class ) );

        // Then
        // Sanity check more than anything else, to make sure the failed update below will retain the value
        assertEquals( 0, store.currentTerm() );

        // When
        try
        {
            store.update( 2 );
            fail( "Test setup should have caused an exception here" );
        }
        catch ( Exception e )
        {
        }

        // Then
        assertEquals( 0, store.currentTerm() );
    }

    @Test
    public void shouldFlushAndCloseOnShutdown() throws Throwable
    {
        // Given
        StoreFileChannel channel = mock( StoreFileChannel.class );
        when( channel.read( any( ByteBuffer.class ) ) ).thenReturn( -1 );
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
        when( fsa.open( any( File.class ), anyString() ) ).thenReturn( channel );

        OnDiskTermState store = new OnDiskTermState( fsa, testDir.directory(), 100, mock( Supplier.class ) );

        // When
        // We shut it down
        store.shutdown();

        // Then
        verify( channel ).flush( );
        verify( channel ).close();
    }
}
