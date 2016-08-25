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
package org.neo4j.coreedge.core.consensus.membership;

import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import org.neo4j.coreedge.core.consensus.log.InMemoryRaftLog;
import org.neo4j.coreedge.core.consensus.log.RaftLogEntry;
import org.neo4j.coreedge.core.consensus.outcome.AppendLogEntry;
import org.neo4j.coreedge.core.consensus.outcome.RaftLogCommand;
import org.neo4j.coreedge.core.consensus.outcome.TruncateLogCommand;
import org.neo4j.coreedge.core.state.storage.InMemoryStateStorage;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.time.Clocks;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.coreedge.core.consensus.membership.RaftMembershipState.Marshal;
import static org.neo4j.coreedge.identity.RaftTestMemberSetBuilder.INSTANCE;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class RaftMembershipManagerTest
{
    @Rule
    public LifeRule lifeRule = new LifeRule( true );

    @Test
    public void membershipManagerShouldUseLatestAppendedMembershipSetEntries()
            throws Exception
    {
        // given
        final InMemoryRaftLog log = new InMemoryRaftLog();

        RaftMembershipManager membershipManager = lifeRule.add( raftMembershipManager( log ) );
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

        RaftMembershipManager membershipManager = lifeRule.add( raftMembershipManager( log ) );

        // when
        List<RaftLogCommand> logCommands = asList(
                new AppendLogEntry( 0, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 4 ) ) ),
                new AppendLogEntry( 1, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 5 ) ) ),
                new TruncateLogCommand( 1 )
        );

        for ( RaftLogCommand logCommand : logCommands )
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

        RaftMembershipManager membershipManager = lifeRule.add( raftMembershipManager( log ) );

        // when
        List<RaftLogCommand> logCommands = asList(
                new AppendLogEntry( 0, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 4 ) ) ),
                new AppendLogEntry( 1, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 5 ) ) ),
                new AppendLogEntry( 2, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 6 ) ) ),
                new TruncateLogCommand( 2 )
        );
        for ( RaftLogCommand logCommand : logCommands )
        {
            logCommand.applyTo( log );
        }
        membershipManager.processLog( 0, logCommands );

        // then
        assertEquals( new RaftTestGroup( 1, 2, 3, 5 ).getMembers(), membershipManager.votingMembers() );
    }

    private RaftMembershipManager raftMembershipManager( InMemoryRaftLog log )
    {
        RaftMembershipManager raftMembershipManager = new RaftMembershipManager(
                null, INSTANCE, log,
                getInstance(), 3, 1000, Clocks.fakeClock(),
                1000, new InMemoryStateStorage<>( new Marshal().startState() ) );

        raftMembershipManager.setRecoverFromIndexSupplier( () -> 0 );
        return raftMembershipManager;
    }
}
