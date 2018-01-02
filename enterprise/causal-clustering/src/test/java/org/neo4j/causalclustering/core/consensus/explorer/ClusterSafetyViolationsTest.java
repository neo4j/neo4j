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
package org.neo4j.causalclustering.core.consensus.explorer;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.identity.MemberId;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.causalclustering.core.consensus.explorer.ClusterSafetyViolations.inconsistentCommittedLogEntries;
import static org.neo4j.causalclustering.core.consensus.explorer.ClusterSafetyViolations.multipleLeadersInSameTerm;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class ClusterSafetyViolationsTest
{
    @Test
    public void shouldRecogniseInconsistentCommittedContent() throws Exception
    {
        // given
        ClusterState clusterState = new ClusterState( asSet( member( 0 ), member( 1 ) ) );

        clusterState.states.get( member( 0 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );
        clusterState.states.get( member( 1 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );

        clusterState.states.get( member( 0 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 2 ) ) );
        clusterState.states.get( member( 1 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 3 ) ) );

        commit( clusterState, member( 0 ), 0 );
        commit( clusterState, member( 1 ), 0 );

        // then
        assertFalse( inconsistentCommittedLogEntries( clusterState ) );

        // when
        commit( clusterState, member( 0 ), 1 );
        commit( clusterState, member( 1 ), 1 );

        // then
        assertTrue( inconsistentCommittedLogEntries( clusterState ) );
    }

    @Test
    public void shouldRecogniseInconsistentTerm() throws Exception
    {
        // given
        ClusterState clusterState = new ClusterState( asSet( member( 0 ), member( 1 ) ) );

        clusterState.states.get( member( 0 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );
        clusterState.states.get( member( 1 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );

        clusterState.states.get( member( 0 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 2 ) ) );
        clusterState.states.get( member( 1 ) ).entryLog.append( new RaftLogEntry( 2, valueOf( 2 ) ) );

        commit( clusterState, member( 0 ), 0 );
        commit( clusterState, member( 1 ), 0 );

        // then
        assertFalse( inconsistentCommittedLogEntries( clusterState ) );

        // when
        commit( clusterState, member( 0 ), 1 );
        commit( clusterState, member( 1 ), 1 );

        // then
        assertTrue( inconsistentCommittedLogEntries( clusterState ) );
    }

    @Test
    public void shouldRecogniseSomeMembersBeingInconsistent() throws Exception
    {
        // given
        ClusterState clusterState = new ClusterState( asSet( member( 0 ), member( 1 ), member(2) ) );

        clusterState.states.get( member( 0 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );
        clusterState.states.get( member( 1 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );
        clusterState.states.get( member( 2 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );

        clusterState.states.get( member( 0 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 2 ) ) );
        clusterState.states.get( member( 1 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 2 ) ) );
        clusterState.states.get( member( 2 ) ).entryLog.append( new RaftLogEntry( 2, valueOf( 2 ) ) );

        commit( clusterState, member( 0 ), 0 );
        commit( clusterState, member( 1 ), 0 );
        commit( clusterState, member( 2 ), 0 );

        // then
        assertFalse( inconsistentCommittedLogEntries( clusterState ) );

        // when
        commit( clusterState, member( 0 ), 1 );
        commit( clusterState, member( 1 ), 1 );

        // then
        assertFalse( inconsistentCommittedLogEntries( clusterState ) );

        // when
        commit( clusterState, member( 2 ), 1 );

        // then
        assertTrue( inconsistentCommittedLogEntries( clusterState ) );
    }

    @Test
    public void shouldRecogniseTwoLeadersInTheSameTerm() throws Exception
    {
        // given
        ClusterState clusterState = new ClusterState( asSet( member( 0 ), member( 1 ), member(2) ) );

        // when
        clusterState.states.get( member( 0 ) ).term = 21;
        clusterState.states.get( member( 1 ) ).term = 21;
        clusterState.states.get( member( 2 ) ).term = 21;

        clusterState.roles.put( member( 0 ), Role.LEADER );
        clusterState.roles.put( member( 1 ), Role.LEADER );
        clusterState.roles.put( member( 2 ), Role.FOLLOWER );

        // then
        assertTrue( multipleLeadersInSameTerm( clusterState ) );
    }

    @Test
    public void shouldRecogniseTwoLeadersInDifferentTerms() throws Exception
    {
        // given
        ClusterState clusterState = new ClusterState( asSet( member( 0 ), member( 1 ), member(2) ) );

        // when
        clusterState.states.get( member( 0 ) ).term = 21;
        clusterState.states.get( member( 1 ) ).term = 22;
        clusterState.states.get( member( 2 ) ).term = 21;

        clusterState.roles.put( member( 0 ), Role.LEADER );
        clusterState.roles.put( member( 1 ), Role.LEADER );
        clusterState.roles.put( member( 2 ), Role.FOLLOWER );

        // then
        assertFalse( multipleLeadersInSameTerm( clusterState ) );
    }

    private void commit( ClusterState clusterState, MemberId member, long commitIndex ) throws IOException
    {
        ComparableRaftState state = clusterState.states.get( member );
        Outcome outcome = new Outcome( clusterState.roles.get( member ), state );
        outcome.setCommitIndex( commitIndex );
        state.update( outcome );
    }
}
