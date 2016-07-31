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
package org.neo4j.coreedge.core.consensus.vote;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.UUID;

import org.neo4j.coreedge.core.consensus.roles.Voting;
import org.neo4j.coreedge.identity.MemberId;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class VotingTest
{
    MemberId candidate = new MemberId( UUID.randomUUID() );
    MemberId otherMember = new MemberId( UUID.randomUUID() );

    long logTerm = 10;
    long currentTerm = 20;
    long appendIndex = 1000;

    @Test
    public void shouldAcceptRequestWithIdenticalLog()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                null
        ) );
    }

    @Test
    public void shouldRejectRequestFromOldTerm()
    {
        assertFalse( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm - 1,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                null
        ) );
    }

    @Test
    public void shouldRejectRequestIfCandidateLogEndsAtLowerTerm()
    {
        assertFalse( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm - 1,
                appendIndex,
                appendIndex,
                null
        ) );
    }

    @Test
    public void shouldRejectRequestIfLogsEndInSameTermButCandidateLogIsShorter()
    {
        assertFalse( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex - 1,
                null
        ) );
    }

    @Test
    public void shouldAcceptRequestIfLogsEndInSameTermAndCandidateLogIsSameLength()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                null
        ) );
    }

    @Test
    public void shouldAcceptRequestIfLogsEndInSameTermAndCandidateLogIsLonger()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex + 1,
                null
        ) );
    }

    @Test
    public void shouldAcceptRequestIfLogsEndInHigherTermAndCandidateLogIsShorter()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm + 1,
                appendIndex,
                appendIndex - 1,
                null
        ) );
    }

    @Test
    public void shouldAcceptRequestIfLogEndsAtHigherTermAndCandidateLogIsSameLength()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm + 1,
                appendIndex,
                appendIndex,
                null
        ) );
    }

    @Test
    public void shouldAcceptRequestIfLogEndsAtHigherTermAndCandidateLogIsLonger()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm + 1,
                appendIndex,
                appendIndex + 1,
                null
        ) );
    }

    @Test
    public void shouldRejectRequestIfAlreadyVotedForOtherCandidate()
    {
        assertFalse( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                otherMember
        ) );
    }

    @Test
    public void shouldAcceptRequestIfAlreadyVotedForCandidate()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                candidate
        ) );
    }
}
