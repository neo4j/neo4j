/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.vote;

import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

import org.neo4j.causalclustering.identity.MemberId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
        assertNull( voteState.votedFor() );
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
        assertNull( voteState.votedFor() );
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
        assertTrue( voteState.update( null, 0 ) );
        assertFalse( voteState.update( null, 0 ) );
        assertTrue( voteState.update( member1, 0 ) );
        assertFalse( voteState.update( member1, 0 ) );
        assertTrue( voteState.update( member2, 1 ) );
        assertFalse( voteState.update( member2, 1 ) );
    }
}
