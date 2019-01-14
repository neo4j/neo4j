/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.causalclustering.core.consensus.roles;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.RaftMachineBuilder;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.membership.RaftTestGroup;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;
import org.neo4j.causalclustering.messaging.Outbound;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesRequest;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesResponse;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

@RunWith( MockitoJUnitRunner.class )
public class AppendEntriesMessageFlowTest
{
    private MemberId myself = member( 0 );
    private MemberId otherMember = member( 1 );

    private ReplicatedInteger data = ReplicatedInteger.valueOf( 1 );

    @Mock
    private Outbound<MemberId, RaftMessages.RaftMessage> outbound;

    ReplicatedInteger data( int value )
    {
        return ReplicatedInteger.valueOf( value );
    }

    private RaftMachine raft;

    @Before
    public void setup() throws IOException
    {
        // given
        RaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, new RaftTestGroup( 0 ) ) );

        raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .raftLog( raftLog )
                .outbound( outbound )
                .build();
    }

    @Test
    public void shouldReturnFalseOnAppendRequestFromOlderTerm() throws Exception
    {
        // when
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( -1 ).prevLogIndex( 0 )
                .prevLogTerm( 0 ).leaderCommit( 0 ).build() );

        // then
        verify( outbound ).send( same( otherMember ),
                eq( appendEntriesResponse().from( myself ).term( 0 ).appendIndex( 0 ).matchIndex( -1 ).failure()
                        .build() ) );
    }

    @Test
    public void shouldReturnTrueOnAppendRequestWithFirstLogEntry() throws Exception
    {
        // when
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 1 ).prevLogIndex( 0 )
                .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 1, data ) ).leaderCommit( -1 ).build() );

        // then
        verify( outbound ).send( same( otherMember ), eq( appendEntriesResponse().
                appendIndex( 1 ).matchIndex( 1 ).from( myself ).term( 1 ).success().build() ) );
    }

    @Test
    public void shouldReturnFalseOnAppendRequestWhenPrevLogEntryNotMatched() throws Exception
    {
        // when
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 0 ).prevLogIndex( 0 )
                .prevLogTerm( 1 ).logEntry( new RaftLogEntry( 0, data ) ).build() );

        // then
        verify( outbound ).send( same( otherMember ),
                eq( appendEntriesResponse().matchIndex( -1 ).appendIndex( 0 ).from( myself ).term( 0 ).failure().build() ) );
    }

    @Test
    public void shouldAcceptSequenceOfAppendEntries() throws Exception
    {
        // when
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 0 ).prevLogIndex( 0 )
                .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 0, data( 1 ) ) ).leaderCommit( -1 ).build() );
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 0 ).prevLogIndex( 1 )
                .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 0, data( 2 ) ) ).leaderCommit( -1 ).build() );
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 0 ).prevLogIndex( 2 )
                .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 0, data( 3 ) ) ).leaderCommit( 0 ).build() );

        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 1 ).prevLogIndex( 3 )
                .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 1, data( 4 ) ) ).leaderCommit( 1 ).build() );
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 1 ).prevLogIndex( 4 )
                .prevLogTerm( 1 ).logEntry( new RaftLogEntry( 1, data( 5 ) ) ).leaderCommit( 2 ).build() );
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 1 ).prevLogIndex( 5 )
                .prevLogTerm( 1 ).logEntry( new RaftLogEntry( 1, data( 6 ) ) ).leaderCommit( 4 ).build() );

        // then
        InOrder invocationOrder = inOrder( outbound );
        invocationOrder.verify( outbound, times( 1 ) ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 0 ).appendIndex( 1 ).matchIndex( 1 ).success().build() ) );
        invocationOrder.verify( outbound, times( 1 ) ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 0 ).appendIndex( 2 ).matchIndex( 2 ).success().build() ) );
        invocationOrder.verify( outbound, times( 1 ) ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 0 ).appendIndex( 3 ).matchIndex( 3 ).success().build() ) );

        invocationOrder.verify( outbound, times( 1 ) ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 1 ).appendIndex( 4 ).matchIndex( 4 ).success().build() ) );
        invocationOrder.verify( outbound, times( 1 ) ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 1 ).appendIndex( 5 ).matchIndex( 5 ).success().build() ) );
        invocationOrder.verify( outbound, times( 1 ) ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 1 ).appendIndex( 6 ).matchIndex( 6 ).success().build() ) );
    }

    @Test
    public void shouldReturnFalseIfLogHistoryDoesNotMatch() throws Exception
    {
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 0 ).prevLogIndex( 0 )
                .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 0, data( 1 ) ) ).build() );
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 0 ).prevLogIndex( 1 )
                .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 0, data( 2 ) ) ).build() );
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 0 ).prevLogIndex( 2 )
                .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 0, data( 3 ) ) ).build() ); // will conflict

        // when
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 2 ).prevLogIndex( 3 )
                .prevLogTerm( 1 ).logEntry( new RaftLogEntry( 2, data( 4 ) ) ).build() ); // should reply false because of prevLogTerm

        // then
        InOrder invocationOrder = inOrder( outbound );
        invocationOrder.verify( outbound, times( 1 ) ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 0 ).matchIndex( 1 ).appendIndex( 1 ).success().build() ) );
        invocationOrder.verify( outbound, times( 1 ) ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 0 ).matchIndex( 2 ).appendIndex( 2 ).success().build() ) );
        invocationOrder.verify( outbound, times( 1 ) ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 0 ).matchIndex( 3 ).appendIndex( 3 ).success().build() ) );
        invocationOrder.verify( outbound, times( 1 ) ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 2 ).matchIndex( -1 ).appendIndex( 3 ).failure().build() ) );
    }
}
