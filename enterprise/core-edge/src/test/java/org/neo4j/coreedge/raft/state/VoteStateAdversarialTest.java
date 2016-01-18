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
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.state.vote.OnDiskVoteState;
import org.neo4j.coreedge.raft.state.vote.VoteState;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.SelectiveFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VoteStateAdversarialTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldDiscardVoteIfChannelFails() throws Exception
    {
        ClassGuardedAdversary adversary = new ClassGuardedAdversary( new CountingAdversary( 1, false ),
                OnDiskVoteState.class );
        adversary.disable();

        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        FileSystemAbstraction fileSystem = new SelectiveFileSystemAbstraction(
                new File( testDir.directory(), "vote.a" ), new AdversarialFileSystemAbstraction( adversary, fs ), fs );
        VoteState<CoreMember> store = createVoteStore( fileSystem );

        final CoreMember member1 = new CoreMember( new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ) );
        final CoreMember member2 = new CoreMember( new AdvertisedSocketAddress( "host2:1001" ),
                new AdvertisedSocketAddress( "host2:2001" ) );

        store.votedFor( member1, 0 );
        adversary.enable();

        try
        {
            store.votedFor( member2, 1 );
            fail( "Should have thrown exception" );
        }
        catch ( RaftStorageException e )
        {
            // expected
        }

        verifyCurrentLogAndNewLogLoadedFromFileSystem( store, fileSystem,
                theStore -> assertEquals( member1, theStore.votedFor() ) );
    }

    private interface VoteVerifier
    {
        void verifyVote( VoteState<CoreMember> store ) throws RaftStorageException;
    }

    private void verifyCurrentLogAndNewLogLoadedFromFileSystem(
            VoteState<CoreMember> store, FileSystemAbstraction fileSystem, VoteVerifier voteVerifier )
            throws RaftStorageException, IOException
    {
        voteVerifier.verifyVote( store );
        voteVerifier.verifyVote( createVoteStore( fileSystem ) );
    }


    private VoteState<CoreMember> createVoteStore( FileSystemAbstraction fileSystem ) throws IOException
    {
        final Supplier mock = mock( Supplier.class );
        when( mock.get() ).thenReturn( mock( DatabaseHealth.class ) );

        return new OnDiskVoteState<>( fileSystem, testDir.directory(), 100, mock,
                new CoreMember.CoreMemberMarshal() );
    }
}
