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

import org.neo4j.coreedge.raft.state.vote.OnDiskVoteState;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.CoreMember.CoreMemberMarshal;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OnDiskVoteStateTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldCallWriteAndFlushOnVoteUpdate() throws Exception
    {
        // Given
        StoreFileChannel channel = mock( StoreFileChannel.class );
        when( channel.read( any( ByteBuffer.class ) ) ).thenReturn( -1 );
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
        when( fsa.open( any( File.class ), anyString() ) ).thenReturn( channel );

        // This has to be real because it will be serialized
        AdvertisedSocketAddress localhost = new AdvertisedSocketAddress( "localhost:" + 1234 );
        CoreMember member = new CoreMember( localhost, localhost );

        OnDiskVoteState<CoreMember> state = new OnDiskVoteState<>( fsa,
                new File( testDir.directory(), "on.disk.state" ), 100, mock( Supplier.class ),
                new CoreMemberMarshal() );

        // When
        state.votedFor( member, 0 );

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
        // Mock the first call to succeed, so we can first store a proper value
        when( channel.write( any( ByteBuffer.class ) ) ).thenReturn( 99 ).thenThrow( new IOException() );

        final Supplier databaseHealthSupplier = mock( Supplier.class );
        when( databaseHealthSupplier.get() ).thenReturn( mock( DatabaseHealth.class ) );

        OnDiskVoteState<CoreMember> state = new OnDiskVoteState<>( fsa,
                new File( testDir.directory(), "on.disk.state" ), 100, databaseHealthSupplier,
                new CoreMemberMarshal() );

        // This has to be real because it will be serialized
        AdvertisedSocketAddress firstLocalhost = new AdvertisedSocketAddress( "localhost:" + 1234 );
        CoreMember firstMember = new CoreMember( firstLocalhost, firstLocalhost );

        // When
        // We do the first store successfully, so we can meaningfully compare the stored value after the failed
        // invocation
        state.votedFor( firstMember, 0 );

        // Then
        assertEquals( firstMember, state.votedFor() );

        // This should not be stored
        AdvertisedSocketAddress secondLocalhost = new AdvertisedSocketAddress( "localhost:" + 1235 );
        CoreMember secondMember = new CoreMember( secondLocalhost, secondLocalhost );

        // When
        try
        {
            state.votedFor( secondMember, 1 );
            fail( "Test setup should have caused an exception here" );
        }
        catch ( Exception e )
        {
            // Then
            // The stored member should not be updated
            assertEquals( firstMember, state.votedFor() );
        }
    }

    @Test
    public void shouldFlushAndCloseOnShutdown() throws Throwable
    {
        // Given
        StoreFileChannel channel = mock( StoreFileChannel.class );
        when( channel.read( any( ByteBuffer.class ) ) ).thenReturn( -1 );

        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
        when( fsa.open( any( File.class ), anyString() ) ).thenReturn( channel );

        OnDiskVoteState<CoreMember> state = new OnDiskVoteState<>( fsa,
                new File( testDir.directory(), "on.disk.state" ), 100, mock( Supplier.class ),
                new CoreMemberMarshal() );

        // When
        state.shutdown();

        // Then
        verify( channel ).flush();
        verify( channel ).close();
    }
}
