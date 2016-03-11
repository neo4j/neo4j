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

import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import org.neo4j.coreedge.catchup.Sleeper;
import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.state.RaftState;
import org.neo4j.coreedge.raft.state.RaftStateBuilder;
import org.neo4j.coreedge.raft.state.ReadableRaftState;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OnDemandJobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.server.RaftTestMember.member;
import static org.neo4j.coreedge.raft.ReplicatedInteger.valueOf;

public class MembershipWaiterTest
{
    @Test
    public void shouldReturnImmediatelyIfMemberAndCaughtUp() throws Exception
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        MembershipWaiter<RaftTestMember> waiter = new MembershipWaiter<>( member( 0 ), jobScheduler, 500,
                NullLogProvider.getInstance(), new LeaderCommitWaiter<>( new NoSleepSleeper() ) );

        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        raftLog.commit( 0 );
        RaftState<RaftTestMember> raftState = RaftStateBuilder.raftState()
                .votingMembers( member( 0 ) )
                .leaderCommit( 0 )
                .entryLog( raftLog )
                .build();

        CompletableFuture<Boolean> future = waiter.waitUntilCaughtUpMember( raftState );
        jobScheduler.runJob();
        jobScheduler.runJob();

        future.get( 0, NANOSECONDS );
    }

    @Test
    public void shouldWaitUntilLeaderCommitIsAvailable() throws Exception
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        MembershipWaiter<RaftTestMember> waiter = new MembershipWaiter<>( member( 0 ), jobScheduler, 500,
                NullLogProvider.getInstance(), new LeaderCommitWaiter<>( new NoSleepSleeper() ) );

        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        raftLog.commit( 0 );

        RaftState<RaftTestMember> raftState = mock( RaftState.class );
        when( raftState.leaderCommit() ).thenReturn( -1L, -1L, 0L, 0L );

        when( raftState.entryLog() ).thenReturn( raftLog );

        HashSet<RaftTestMember> members = new HashSet<>();
        members.add( member( 0 ) );
        when( raftState.votingMembers() ).thenReturn( members );

        CompletableFuture<Boolean> future = waiter.waitUntilCaughtUpMember( raftState );
        jobScheduler.runJob();
        jobScheduler.runJob();

        future.get( 1, TimeUnit.SECONDS );
    }

    @Test
    public void shouldTimeoutIfCaughtUpButNotMember() throws Exception
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        MembershipWaiter<RaftTestMember> waiter = new MembershipWaiter<>( member( 0 ), jobScheduler, 1,
                NullLogProvider.getInstance(), new LeaderCommitWaiter<>( new NoSleepSleeper() ) );

        RaftState<RaftTestMember> raftState = RaftStateBuilder.raftState()
                .votingMembers( member( 1 ) )
                .leaderCommit( 0 )
                .build();

        CompletableFuture<Boolean> future = waiter.waitUntilCaughtUpMember( raftState );
        jobScheduler.runJob();
        jobScheduler.runJob();

        try
        {
            future.get( 10, MILLISECONDS );
            fail( "Should have timed out." );
        }
        catch ( TimeoutException e )
        {
            // expected
        }
    }

    @Test
    public void shouldTimeoutIfMemberButNotCaughtUp() throws Exception
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        MembershipWaiter<RaftTestMember> waiter = new MembershipWaiter<>( member( 0 ), jobScheduler, 1,
                NullLogProvider.getInstance(), new LeaderCommitWaiter<>( new NoSleepSleeper() ) );

        RaftState<RaftTestMember> raftState = RaftStateBuilder.raftState()
                .votingMembers( member( 0 ), member( 1 ) )
                .leaderCommit( 0 )
                .build();

        CompletableFuture<Boolean> future = waiter.waitUntilCaughtUpMember( raftState );
        jobScheduler.runJob();
        jobScheduler.runJob();

        try
        {
            future.get( 10, MILLISECONDS );
            fail( "Should have timed out." );
        }
        catch ( TimeoutException e )
        {
            // expected
        }
    }

    @Test
    public void shouldTimeoutIfLeaderCommitIsNeverKnown() throws Exception
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        MembershipWaiter<RaftTestMember> waiter = new MembershipWaiter<>( member( 0 ), jobScheduler, 1,
                NullLogProvider.getInstance(), new LimitedTriesLeaderCommitWaiter<>( new NoSleepSleeper(), 1 ) );

        RaftState<RaftTestMember> raftState = RaftStateBuilder.raftState()
                .leaderCommit( -1L )
                .build();

        CompletableFuture<Boolean> future = waiter.waitUntilCaughtUpMember( raftState );
        jobScheduler.runJob();

        try
        {
            future.get( 10, MILLISECONDS );
            fail( "Should have timed out." );
        }
        catch ( TimeoutException e )
        {
            // expected
        }
    }

    private class NoSleepSleeper implements Sleeper
    {
        @Override
        public void sleep( long millis )
        {
            // no need to sleep, only for tests
        }
    }

    class LimitedTriesLeaderCommitWaiter<MEMBER> extends LeaderCommitWaiter<MEMBER>
    {
        private int attempts;

        public LimitedTriesLeaderCommitWaiter( Sleeper sleeper, int attempts )
        {
            super( sleeper );
            this.attempts = attempts;
        }

        @Override
        public void waitMore()
        {
            attempts--;
        }

        @Override
        public boolean keepWaiting( ReadableRaftState<MEMBER> raftState )
        {
            return super.keepWaiting( raftState ) && attempts > 0;
        }
    }

}