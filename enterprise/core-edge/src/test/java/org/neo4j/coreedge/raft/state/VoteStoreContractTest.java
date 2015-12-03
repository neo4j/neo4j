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

import org.junit.Test;

import org.neo4j.coreedge.server.CoreMember;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;

public abstract class VoteStoreContractTest
{
    public abstract VoteStore<CoreMember> createVoteStore();

    @Test
    public void shouldStoreVote() throws Exception
    {
        // given
        VoteStore<CoreMember> voteStore = createVoteStore();
        CoreMember member = new CoreMember( address( "host1:1001" ), address( "host1:2001" ) );

        // when
        voteStore.update( member );

        // then
        assertEquals( member, voteStore.votedFor() );
    }

    @Test
    public void shouldStartWithNoVote() throws Exception
    {
        // given
        VoteStore<CoreMember> voteStore = createVoteStore();

        // then
        assertNull( voteStore.votedFor() );
    }

    @Test
    public void shouldUpdateVote() throws Exception
    {
        // given
        VoteStore<CoreMember> voteStore = createVoteStore();
        CoreMember member1 = new CoreMember( address( "host1:1001" ), address( "host1:2001" ) );
        CoreMember member2 = new CoreMember( address( "host2:1001" ), address( "host2:2001" ) );

        // when
        voteStore.update( member1 );
        voteStore.update( member2 );

        // then
        assertEquals( member2, voteStore.votedFor() );
    }

    @Test
    public void shouldClearVote() throws Exception
    {
        // given
        VoteStore<CoreMember> voteStore = createVoteStore();
        CoreMember member = new CoreMember( address( "host1:1001" ), address( "host1:2001" ) );
        voteStore.update( member );

        // when
        voteStore.update( null );

        // then
        assertNull( voteStore.votedFor() );
    }
}