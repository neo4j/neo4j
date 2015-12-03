/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreFileChannel;

public class DurableVoteStoreTest
{
    @Test
    public void shouldCallWriteAllAndForceOnVoteUpdate() throws Exception
    {
        // Given
        StoreFileChannel channel = mock( StoreFileChannel.class );
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
        when( fsa.open( any( File.class ), anyString() ) ).thenReturn( channel );

        // This has to be real because it will be serialized
        AdvertisedSocketAddress localhost = new AdvertisedSocketAddress( InetSocketAddress.createUnresolved(
                "localhost", 1234 ) );
        CoreMember member = new CoreMember( localhost, localhost );

        DurableVoteStore store = new DurableVoteStore( fsa, new File("") );

        // When
        store.update( member );

        // Then
        verify( channel ).writeAll( any( ByteBuffer.class ), anyInt() );
        verify( channel ).force( anyBoolean() );
    }

    @Test
    public void termShouldRemainUnchangedOnFailureToWriteToDisk() throws Exception
    {
        // Given
        StoreFileChannel channel = mock( StoreFileChannel.class );
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
        when( fsa.open( any( File.class ), anyString() ) ).thenReturn( channel );
        // Mock the first call to succeed, so we can first store a proper value
        doNothing().doThrow( new IOException() ).when( channel ).writeAll( any( ByteBuffer.class ), anyInt() );

        DurableVoteStore store = new DurableVoteStore( fsa, new File("") );

        // This has to be real because it will be serialized
        AdvertisedSocketAddress firstLocalhost = new AdvertisedSocketAddress( InetSocketAddress.createUnresolved(
                "localhost", 1234 ) );
        CoreMember firstMember = new CoreMember( firstLocalhost, firstLocalhost );

        // When
        // We do the first store successfully, so we can meaningfully compare the stored value after the failed invocation
        store.update( firstMember );

        // Then
        assertEquals( firstMember, store.votedFor() );

        // This should not be stored
        AdvertisedSocketAddress secondLocalhost = new AdvertisedSocketAddress( InetSocketAddress.createUnresolved(
                "localhost", 1235 ) );
        CoreMember secondMember = new CoreMember( secondLocalhost, secondLocalhost );

        // When
        try
        {
            store.update( secondMember);
            fail( "Test setup should have caused an exception here");
        }
        catch( Exception e )
        {}

        // Then
        // The stored member should not be updated
        assertEquals( firstMember, store.votedFor() );
    }

    @Test
    public void shouldForceAndCloseOnShutdown() throws Throwable
    {
        // Given
        StoreFileChannel channel = mock( StoreFileChannel.class );
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
        when( fsa.open( any( File.class ), anyString() ) ).thenReturn( channel );

        DurableVoteStore store = new DurableVoteStore( fsa, new File("") );

        // When
        // We shut it down
        store.shutdown();

        // Then
        verify( channel ).force( false );
        verify( channel ).close();
    }
}