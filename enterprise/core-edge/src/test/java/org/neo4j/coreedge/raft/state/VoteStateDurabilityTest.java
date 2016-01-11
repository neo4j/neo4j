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

import org.junit.Test;

import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.state.vote.OnDiskVoteState;
import org.neo4j.coreedge.raft.state.vote.VoteState;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class VoteStateDurabilityTest
{
    public VoteState<CoreMember> createVoteStore( EphemeralFileSystemAbstraction fileSystem ) throws IOException
    {
        File directory = new File( "raft-log" );
        fileSystem.mkdir( directory );
        return new OnDiskVoteState<>( fileSystem, directory, 100, mock(Supplier.class), new CoreMember.CoreMemberMarshal() );
    }

    @Test
    public void shouldStoreVote() throws Exception
    {
        EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
        VoteState<CoreMember> voteState = createVoteStore( fileSystem );

        final CoreMember member = new CoreMember( new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ) );
        voteState.votedFor( member, 0 );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( voteState, fileSystem,
                voteStore1 -> assertEquals( member, voteStore1.votedFor() ) );
    }

    @Test
    public void emptyFileShouldImplyNoVoteCast() throws Exception
    {
        EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
        VoteState<CoreMember> voteState = createVoteStore( fileSystem );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( voteState, fileSystem,
                voteStore1 -> assertNull( voteStore1.votedFor() ) );
    }

    private void verifyCurrentLogAndNewLogLoadedFromFileSystem( VoteState<CoreMember> voteState,
                                                                EphemeralFileSystemAbstraction fileSystem,
                                                                VoteVerifier voteVerifier ) throws RaftStorageException, IOException
    {
        voteVerifier.verifyVote( voteState );
        voteVerifier.verifyVote( createVoteStore( fileSystem ) );
        fileSystem.crash();
        voteVerifier.verifyVote( createVoteStore( fileSystem ) );
    }

    private interface VoteVerifier
    {
        void verifyVote( VoteState<CoreMember> voteState ) throws RaftStorageException;
    }
}
