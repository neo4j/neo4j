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
package org.neo4j.coreedge.raft.membership;

import org.junit.Test;

import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.replication.StubReplicator;
import org.neo4j.coreedge.raft.state.membership.InMemoryRaftMembershipState;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.server.RaftTestMemberSetBuilder;
import org.neo4j.helpers.FakeClock;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RaftMembershipManagerTest
{
    @Test
    public void membershipManagerShouldUseLatestAppendedMembershipSetEntries()
            throws Exception
    {
        // given
        final InMemoryRaftLog log = new InMemoryRaftLog();

        RaftMembershipManager<RaftTestMember> membershipManager = new RaftMembershipManager<>( new StubReplicator(),
                RaftTestMemberSetBuilder.INSTANCE, log, NullLogProvider.getInstance(), 3, 1000, new FakeClock(),
                1000, new InMemoryRaftMembershipState<>() );

        log.registerListener( membershipManager );

        // when
        log.append( new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 4 ) ) );
        log.commit( 0 );
        log.append( new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 5 ) ) );

        // then
        assertEquals( new RaftTestGroup( 1, 2, 3, 5 ).getMembers(), membershipManager.votingMembers() );
    }

    @Test
    public void membershipManagerShouldRevertToOldMembershipSetAfterTruncationCausesLossOfAllAppendedMembershipSets()
            throws Exception
    {
        // given
        final InMemoryRaftLog log = new InMemoryRaftLog();


        RaftMembershipManager<RaftTestMember> membershipManager = new RaftMembershipManager<>( new StubReplicator(),
                RaftTestMemberSetBuilder.INSTANCE, log, NullLogProvider.getInstance(), 3, 1000, new FakeClock(),
                1000, new InMemoryRaftMembershipState<>() );

        log.registerListener( membershipManager );

        log.append( new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 4 ) ) );
        log.commit( 0 );
        log.append( new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 5 ) ) );

        // when
        log.truncate( log.appendIndex() );

        // then
        assertEquals( new RaftTestGroup( 1, 2, 3, 4 ).getMembers(), membershipManager.votingMembers() );
        assertFalse( membershipManager.uncommittedMemberChangeInLog() );
    }

    @Test
    public void membershipManagerShouldRevertToEarlierAppendedMembershipSetAfterTruncationCausesLossOfLastAppened()
            throws Exception
    {
        // given
        final InMemoryRaftLog log = new InMemoryRaftLog();

        RaftMembershipManager<RaftTestMember> membershipManager = new RaftMembershipManager<>( new StubReplicator(),
                RaftTestMemberSetBuilder.INSTANCE, log, NullLogProvider.getInstance(), 3, 1000, new FakeClock(),
                1000, new InMemoryRaftMembershipState<>() );

        log.registerListener( membershipManager );

        log.append( new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 4 ) ) );
        log.commit( 0 );
        log.append( new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 5 ) ) );
        log.append( new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 6 ) ) );

        // when
        log.truncate( log.appendIndex() );

        // then
        assertEquals( new RaftTestGroup( 1, 2, 3, 5 ).getMembers(), membershipManager.votingMembers() );
        assertTrue( membershipManager.uncommittedMemberChangeInLog() );
    }

    @Test
    public void shouldNotOverwriteCurrentStateWithPreviousState() throws Exception
    {
        // given
        final InMemoryRaftLog log = new InMemoryRaftLog();

        final InMemoryRaftMembershipState<RaftTestMember> state = mock( InMemoryRaftMembershipState.class );
        final long logIndex = 42l;
        when( state.logIndex() ).thenReturn( logIndex );

        RaftMembershipManager<RaftTestMember> membershipManager = new RaftMembershipManager<>( new StubReplicator(),
                RaftTestMemberSetBuilder.INSTANCE, log, NullLogProvider.getInstance(), 3, 1000, new FakeClock(),
                1000, state );

        log.registerListener( membershipManager );

        // when
        membershipManager.onAppended( new RaftTestGroup( 1, 2, 3, 4 ), logIndex - 1 );

        // then
        verify( state, times( 0 ) ).logIndex( anyLong() );
        verify( state, times( 0 ) ).setVotingMembers( anySet() );
    }
}