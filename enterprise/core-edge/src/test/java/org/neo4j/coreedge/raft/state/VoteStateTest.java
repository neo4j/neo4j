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
import org.neo4j.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class VoteStateTest
{
    @Test
    public void shouldStoreVote() throws Exception
    {
        // given
        VoteState<CoreMember> voteState = new VoteState<>();
        CoreMember member = new CoreMember(
                new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ),
                new AdvertisedSocketAddress( "host1:3001" ));

        // when
        voteState.update( member, 0 );

        // then
        assertEquals( member, voteState.votedFor() );
    }

    @Test
    public void shouldStartWithNoVote() throws Exception
    {
        // given
        VoteState<CoreMember> voteState = new VoteState<>();

        // then
        assertNull( voteState.votedFor() );
    }

    @Test
    public void shouldUpdateVote() throws Exception
    {
        // given
        VoteState<CoreMember> voteState = new VoteState<>();
        CoreMember member1 = new CoreMember( new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ), new AdvertisedSocketAddress( "host1:3001" )
        );
        CoreMember member2 = new CoreMember( new AdvertisedSocketAddress( "host2:1001" ),
                new AdvertisedSocketAddress( "host2:2001" ), new AdvertisedSocketAddress( "host2:3001" )
        );

        // when
        voteState.update( member1, 0 );
        voteState.update( member2, 1 );

        // then
        assertEquals( member2, voteState.votedFor() );
    }

    @Test
    public void shouldClearVote() throws Exception
    {
        // given
        VoteState<CoreMember> voteState = new VoteState<>();
        CoreMember member = new CoreMember( new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ), new AdvertisedSocketAddress( "host1:2001" )
        );
        voteState.update( member, 0 );

        // when
        voteState.update( null, 1 );

        // then
        assertNull( voteState.votedFor() );
    }

    @Test
    public void shouldNotUpdateVoteForSameTerm() throws Exception
    {
        // given
        VoteState<CoreMember> voteState = new VoteState<>();
        CoreMember member1 = new CoreMember( new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ), new AdvertisedSocketAddress( "host1:3001" )
        );
        CoreMember member2 = new CoreMember( new AdvertisedSocketAddress( "host2:1001" ),
                new AdvertisedSocketAddress( "host2:2001" ), new AdvertisedSocketAddress( "host2:3001" )
        );

        voteState.update( member1, 0 );

        try
        {
            // when
            voteState.update( member2, 0 );
            fail( "Should have thrown IllegalArgumentException" );
        }
        catch ( IllegalArgumentException expected )
        {
            // expected
        }
    }

    @Test
    public void shouldNotClearVoteForSameTerm() throws Exception
    {
        // given
        VoteState<CoreMember> voteState = new VoteState<>();
        CoreMember member1 = new CoreMember( new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ), new AdvertisedSocketAddress( "host1:3001" )
        );

        voteState.update( member1, 0 );

        try
        {
            // when
            voteState.update( null, 0 );
            fail( "Should have thrown IllegalArgumentException" );
        }
        catch ( IllegalArgumentException expected )
        {
            // expected
        }
    }

    @Test
    public void shouldReportNoUpdateWhenVoteStateUnchanged() throws Exception
    {
        // given
        VoteState<CoreMember> voteState = new VoteState<>();
        CoreMember member1 = new CoreMember( new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ), new AdvertisedSocketAddress( "host1:3001" )
        );
        CoreMember member2 = new CoreMember( new AdvertisedSocketAddress( "host2:1001" ),
                new AdvertisedSocketAddress( "host2:2001" ), new AdvertisedSocketAddress( "host2:3001" )
        );

        // when
        assertTrue( voteState.update( null, 0 ) );
        assertFalse( voteState.update( null, 0 ) );
        assertTrue( voteState.update( member1, 0 ) );
        assertFalse( voteState.update( member1, 0 ) );
        assertTrue( voteState.update( member2, 1 ) );
        assertFalse( voteState.update( member2, 1 ) );
    }
}
