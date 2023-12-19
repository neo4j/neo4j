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
package org.neo4j.causalclustering.core.consensus.roles;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.state.RaftState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.heartbeat;
import static org.neo4j.causalclustering.core.consensus.roles.AppendEntriesRequestTest.ContentGenerator.content;
import static org.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.raftState;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

@RunWith( Parameterized.class )
public class HeartbeatTest
{
    @Parameterized.Parameters( name = "{0} with leader {1} terms ahead." )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]{
                {Role.FOLLOWER, 0}, {Role.FOLLOWER, 1}, {Role.LEADER, 1}, {Role.CANDIDATE, 1}
        } );
    }

    @Parameterized.Parameter( value = 0 )
    public Role role;

    @Parameterized.Parameter( value = 1 )
    public int leaderTermDifference;

    private MemberId myself = member( 0 );
    private MemberId leader = member( 1 );

    @Test
    public void shouldNotResultInCommitIfReferringToFutureEntries() throws Exception
    {
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = raftState()
                .myself( myself )
                .entryLog( raftLog )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( leaderTerm, content() ) );

        RaftMessages.Heartbeat heartbeat = heartbeat()
                .from( leader )
                .commitIndex( raftLog.appendIndex() + 1) // The leader is talking about committing stuff we don't know about
                .commitIndexTerm( leaderTerm ) // And is in the same term
                .leaderTerm( leaderTerm )
                .build();

        Outcome outcome = role.handler.handle( heartbeat, state, log() );

        assertThat( outcome.getLogCommands(), empty());
    }

    @Test
    public void shouldNotResultInCommitIfHistoryMismatches() throws Exception
    {
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = raftState()
                .myself( myself )
                .entryLog( raftLog )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( leaderTerm, content() ) );

        RaftMessages.Heartbeat heartbeat = heartbeat()
                .from( leader )
                .commitIndex( raftLog.appendIndex()) // The leader is talking about committing stuff we don't know about
                .commitIndexTerm( leaderTerm ) // And is in the same term
                .leaderTerm( leaderTerm )
                .build();

        Outcome outcome = role.handler.handle( heartbeat, state, log() );

        assertThat( outcome.getCommitIndex(), Matchers.equalTo(0L) );
    }

    @Test
    public void shouldResultInCommitIfHistoryMatches() throws Exception
    {
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = raftState()
                .myself( myself )
                .entryLog( raftLog )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( leaderTerm - 1, content() ) );

        RaftMessages.Heartbeat heartbeat = heartbeat()
                .from( leader )
                .commitIndex( raftLog.appendIndex()) // The leader is talking about committing stuff we don't know about
                .commitIndexTerm( leaderTerm ) // And is in the same term
                .leaderTerm( leaderTerm )
                .build();

        Outcome outcome = role.handler.handle( heartbeat, state, log() );

        assertThat( outcome.getLogCommands(), empty() );

    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }

}
