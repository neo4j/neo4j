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
package org.neo4j.coreedge.raft;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class BallotTest
{
    Object A = new Object();
    Object B = new Object();

    @Test
    public void shouldVoteFalseInOldTerm()
    {
        // when + then
        assertFalse( Ballot.shouldVoteFor( A, 4 /*request term */, 5 /*context term */, -1, -1, -1, -1, null ) );
    }

    @Test
    public void shouldVoteFalseIfLogNotUpToDateBecauseOfTerm()
    {
        // when + then
        assertFalse( Ballot.shouldVoteFor( A, 0, 0, 0, 0, 1 /*context last log term */, 0 /*request last log term */,
                null ) );
    }

    @Test
    public void shouldVoteFalseIfLogNotUpToDateBecauseOfIndex()
    {
        // when + then
        assertFalse( Ballot.shouldVoteFor( A, 0, 0, 1/*context last appended */, 0/*request last log index*/, 0, 0,
                null ) );
    }

    @Test
    public void shouldVoteFalseIfAlreadyVotedForOtherCandidate()
    {
        // when + then
        assertFalse( Ballot.shouldVoteFor( A, 0, 0, 0, 0, 0, 0, B ) );
    }

    @Test
    public void shouldVoteTrueIfAlreadyVotedForCandidate()
    {
        // when + then
        assertTrue( Ballot.shouldVoteFor( A, 0, 0, 0, 0, 0, 0, A ) );
    }

    @Test
    public void shouldVoteTrueForNewCandidateWithUpToDateLog()
    {
        // when + then
        assertTrue( Ballot.shouldVoteFor( A, 0, 0, 0, 0, 0, 0, null ) );
    }
}
