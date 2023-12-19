/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.membership;

import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.outcome.AppendLogEntry;
import org.neo4j.causalclustering.core.consensus.outcome.RaftLogCommand;
import org.neo4j.causalclustering.core.consensus.outcome.TruncateLogCommand;
import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.time.Clocks;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.causalclustering.core.consensus.membership.RaftMembershipState.Marshal;
import static org.neo4j.causalclustering.identity.RaftTestMemberSetBuilder.INSTANCE;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class RaftMembershipManagerTest
{
    private final Log log = NullLog.getInstance();

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
        final InMemoryRaftLog raftLog = new InMemoryRaftLog();

        RaftMembershipManager membershipManager = lifeRule.add( raftMembershipManager( raftLog ) );

        // when
        List<RaftLogCommand> logCommands = asList(
                new AppendLogEntry( 0, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 4 ) ) ),
                new AppendLogEntry( 1, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 5 ) ) ),
                new TruncateLogCommand( 1 )
        );

        for ( RaftLogCommand logCommand : logCommands )
        {
            logCommand.applyTo( raftLog, log );
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
        final InMemoryRaftLog raftLog = new InMemoryRaftLog();

        RaftMembershipManager membershipManager = lifeRule.add( raftMembershipManager( raftLog ) );

        // when
        List<RaftLogCommand> logCommands = asList(
                new AppendLogEntry( 0, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 4 ) ) ),
                new AppendLogEntry( 1, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 5 ) ) ),
                new AppendLogEntry( 2, new RaftLogEntry( 0, new RaftTestGroup( 1, 2, 3, 6 ) ) ),
                new TruncateLogCommand( 2 )
        );
        for ( RaftLogCommand logCommand : logCommands )
        {
            logCommand.applyTo( raftLog, log );
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
