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

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.outcome.AppendLogEntry;
import org.neo4j.coreedge.raft.outcome.LogCommand;
import org.neo4j.coreedge.raft.outcome.TruncateLogCommand;
import org.neo4j.coreedge.raft.state.InMemoryStateStorage;
import org.neo4j.coreedge.raft.state.StateStorage;
import org.neo4j.coreedge.raft.state.membership.RaftMembershipState;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.server.RaftTestMemberSetBuilder;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.FakeClock;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
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

        RaftMembershipManager<RaftTestMember> membershipManager = new RaftMembershipManager<>(
                null, RaftTestMemberSetBuilder.INSTANCE, log,
                NullLogProvider.getInstance(), 3, 1000, new FakeClock(),
                1000, new InMemoryStateStorage<>( new RaftMembershipState<>() ) );

        // when
        membershipManager.processLog( 0, asList(
                new AppendLogEntry( 0, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 4 ) ) ),
                new AppendLogEntry( 1, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 5 ) ) )
        ) );

        // then
        assertEquals( new RaftTestGroup( 1, 2, 3, 5 ).getMembers(), membershipManager.votingMembers() );
    }

    @Test
    public void membershipManagerShouldRevertToOldMembershipSetAfterTruncationCausesLossOfAllAppendedMembershipSets()
            throws Exception
    {
        // given
        final InMemoryRaftLog log = new InMemoryRaftLog();

        RaftMembershipManager<RaftTestMember> membershipManager = new RaftMembershipManager<>(
                null,
                RaftTestMemberSetBuilder.INSTANCE, log, NullLogProvider.getInstance(), 3, 1000, new FakeClock(),
                1000, new InMemoryStateStorage<>( new RaftMembershipState<>() ) );

        // when
        List<LogCommand> logCommands = asList(
                new AppendLogEntry( 0, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 4 ) ) ),
                new AppendLogEntry( 1, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 5 ) ) ),
                new TruncateLogCommand( 1 )
        );

        for ( LogCommand logCommand : logCommands )
        {
            logCommand.applyTo( log );
        }
        membershipManager.processLog( 0, logCommands );

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

        RaftMembershipManager<RaftTestMember> membershipManager = new RaftMembershipManager<>(
                null,
                RaftTestMemberSetBuilder.INSTANCE, log, NullLogProvider.getInstance(), 3, 1000, new FakeClock(),
                1000, new InMemoryStateStorage<>( new RaftMembershipState<>() ) );

        // when
        List<LogCommand> logCommands = asList(
                new AppendLogEntry( 0, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 4 ) ) ),
                new AppendLogEntry( 1, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 5 ) ) ),
                new AppendLogEntry( 2, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 6 ) ) ),
                new TruncateLogCommand( 2 )
        );
        for ( LogCommand logCommand : logCommands )
        {
            logCommand.applyTo( log );
        }
        membershipManager.processLog( 0, logCommands );

        // then
        assertEquals( new RaftTestGroup( 1, 2, 3, 5 ).getMembers(), membershipManager.votingMembers() );
        assertTrue( membershipManager.uncommittedMemberChangeInLog() );
    }

    @Test
    public void shouldNotOverwriteCurrentStateWithPreviousState() throws Exception
    {
        // given
        final InMemoryRaftLog log = new InMemoryRaftLog();

        RaftMembershipState<RaftTestMember> state = new RaftMembershipState<>();
        state.logIndex( 42L );

        final StateStorage<RaftMembershipState<RaftTestMember>> stateStorage = mock( StateStorage.class );
        when( stateStorage.getInitialState() ).thenReturn( state );

        RaftMembershipManager<RaftTestMember> membershipManager = new RaftMembershipManager<>(
                null,
                RaftTestMemberSetBuilder.INSTANCE, log, NullLogProvider.getInstance(), 3, 1000, new FakeClock(),
                1000, stateStorage );

        // when
        membershipManager.processLog( 0, Collections.singletonList( new AppendLogEntry( 0, new RaftLogEntry( 0, new
                RaftTestGroup( 1, 2, 3, 4 ) ) ) ) );

        // then
        verify( stateStorage, times( 0 ) ).persistStoreData( any() );
    }
}
