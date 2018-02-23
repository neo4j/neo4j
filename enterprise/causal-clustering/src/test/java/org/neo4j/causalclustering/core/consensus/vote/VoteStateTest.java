/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.vote;

import java.util.UUID;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import org.neo4j.causalclustering.identity.MemberId;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VoteStateTest
{
    @Test
    public void shouldStoreVote()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member = new MemberId( UUID.randomUUID() );

        // when
        voteState.update( member, 0 );

        // then
        assertEquals( member, voteState.votedFor() );
    }

    @Test
    public void shouldStartWithNoVote()
    {
        // given
        VoteState voteState = new VoteState();

        // then
        Assert.assertNull( voteState.votedFor() );
    }

    @Test
    public void shouldUpdateVote()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member1 = new MemberId( UUID.randomUUID() );
        MemberId member2 = new MemberId( UUID.randomUUID() );

        // when
        voteState.update( member1, 0 );
        voteState.update( member2, 1 );

        // then
        assertEquals( member2, voteState.votedFor() );
    }

    @Test
    public void shouldClearVote()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member = new MemberId( UUID.randomUUID() );

        voteState.update( member, 0 );

        // when
        voteState.update( null, 1 );

        // then
        Assert.assertNull( voteState.votedFor() );
    }

    @Test
    public void shouldNotUpdateVoteForSameTerm()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member1 = new MemberId( UUID.randomUUID() );
        MemberId member2 = new MemberId( UUID.randomUUID() );

        voteState.update( member1, 0 );

        try
        {
            // when
            voteState.update( member2, 0 );
            Assert.fail( "Should have thrown IllegalArgumentException" );
        }
        catch ( IllegalArgumentException expected )
        {
            // expected
        }
    }

    @Test
    public void shouldNotClearVoteForSameTerm()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member = new MemberId( UUID.randomUUID() );

        voteState.update( member, 0 );

        try
        {
            // when
            voteState.update( null, 0 );
            Assert.fail( "Should have thrown IllegalArgumentException" );
        }
        catch ( IllegalArgumentException expected )
        {
            // expected
        }
    }

    @Test
    public void shouldReportNoUpdateWhenVoteStateUnchanged()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member1 = new MemberId( UUID.randomUUID() );
        MemberId member2 = new MemberId( UUID.randomUUID() );

        // when
        Assert.assertTrue( voteState.update( null, 0 ) );
        Assert.assertFalse( voteState.update( null, 0 ) );
        Assert.assertTrue( voteState.update( member1, 0 ) );
        Assert.assertFalse( voteState.update( member1, 0 ) );
        Assert.assertTrue( voteState.update( member2, 1 ) );
        Assert.assertFalse( voteState.update( member2, 1 ) );
    }
}
