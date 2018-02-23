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

import org.junit.jupiter.api.Test;

import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.roles.Voting;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VotingTest
{
    private MemberId candidate = new MemberId( UUID.randomUUID() );

    private long logTerm = 10;
    private long currentTerm = 20;
    private long appendIndex = 1000;

    private Log log = NullLog.getInstance();

    @Test
    void shouldAcceptRequestWithIdenticalLog()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                false,
                log
        ) );
    }

    @Test
    void shouldRejectRequestFromOldTerm()
    {
        assertFalse( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm - 1,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                false,
                log
        ) );
    }

    @Test
    void shouldRejectRequestIfCandidateLogEndsAtLowerTerm()
    {
        assertFalse( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm - 1,
                appendIndex,
                appendIndex,
                false,
                log
        ) );
    }

    @Test
    void shouldRejectRequestIfLogsEndInSameTermButCandidateLogIsShorter()
    {
        assertFalse( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex - 1,
                false,
                log
        ) );
    }

    @Test
    void shouldAcceptRequestIfLogsEndInSameTermAndCandidateLogIsSameLength()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                false,
                log
        ) );
    }

    @Test
    void shouldAcceptRequestIfLogsEndInSameTermAndCandidateLogIsLonger()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex + 1,
                false,
                log
        ) );
    }

    @Test
    void shouldAcceptRequestIfLogsEndInHigherTermAndCandidateLogIsShorter()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm + 1,
                appendIndex,
                appendIndex - 1,
                false,
                log
        ) );
    }

    @Test
    void shouldAcceptRequestIfLogEndsAtHigherTermAndCandidateLogIsSameLength()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm + 1,
                appendIndex,
                appendIndex,
                false,
                log
        ) );
    }

    @Test
    void shouldAcceptRequestIfLogEndsAtHigherTermAndCandidateLogIsLonger()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm + 1,
                appendIndex,
                appendIndex + 1,
                false,
                log
        ) );
    }

    @Test
    void shouldRejectRequestIfAlreadyVotedForOtherCandidate()
    {
        assertFalse( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                true,
                log
        ) );
    }
}
