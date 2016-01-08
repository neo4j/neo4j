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

import org.junit.Test;

import org.neo4j.coreedge.raft.state.vote.VoteState;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public abstract class VoteStoreContractTest
{
    public abstract VoteState<CoreMember> createVoteStore();

    @Test
    public void shouldStoreVote() throws Exception
    {
        // given
        VoteState<CoreMember> voteState = createVoteStore();
        CoreMember member = new CoreMember( new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ) );

        // when
        voteState.votedFor( member );

        // then
        assertEquals( member, voteState.votedFor() );
    }

    @Test
    public void shouldStartWithNoVote() throws Exception
    {
        // given
        VoteState<CoreMember> voteState = createVoteStore();

        // then
        assertNull( voteState.votedFor() );
    }

    @Test
    public void shouldUpdateVote() throws Exception
    {
        // given
        VoteState<CoreMember> voteState = createVoteStore();
        CoreMember member1 = new CoreMember( new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ) );
        CoreMember member2 = new CoreMember( new AdvertisedSocketAddress( "host2:1001" ),
                new AdvertisedSocketAddress( "host2:2001" ) );

        // when
        voteState.votedFor( member1 );
        voteState.votedFor( member2 );

        // then
        assertEquals( member2, voteState.votedFor() );
    }

    @Test
    public void shouldClearVote() throws Exception
    {
        // given
        VoteState<CoreMember> voteState = createVoteStore();
        CoreMember member = new CoreMember( new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ) );
        voteState.votedFor( member );

        // when
        voteState.votedFor( null );

        // then
        assertNull( voteState.votedFor() );
    }
}
