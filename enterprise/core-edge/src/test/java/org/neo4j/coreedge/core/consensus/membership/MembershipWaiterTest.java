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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import org.neo4j.coreedge.core.consensus.MismatchedStoreIdService;
import org.neo4j.coreedge.core.consensus.log.InMemoryRaftLog;
import org.neo4j.coreedge.core.consensus.log.RaftLogEntry;
import org.neo4j.coreedge.core.consensus.state.RaftState;
import org.neo4j.coreedge.core.consensus.state.RaftStateBuilder;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OnDemandJobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import static org.neo4j.coreedge.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.coreedge.identity.RaftTestMember.member;

public class MembershipWaiterTest
{
    @Test
    public void shouldReturnImmediatelyIfMemberAndCaughtUp() throws Exception
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        MembershipWaiter waiter = new MembershipWaiter( member( 0 ), jobScheduler, 500,
                mock( MismatchedStoreIdService.class ), NullLogProvider.getInstance() );

        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        RaftState raftState = RaftStateBuilder.raftState()
                .votingMembers( member( 0 ) )
                .leaderCommit( 0 )
                .entryLog( raftLog )
                .commitIndex( 0L )
                .build();

        CompletableFuture<Boolean> future = waiter.waitUntilCaughtUpMember( raftState );
        jobScheduler.runJob();

        future.get( 0, NANOSECONDS );
    }

    @Test
    public void shouldTimeoutIfCaughtUpButNotMember() throws Exception
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        MembershipWaiter waiter = new MembershipWaiter( member( 0 ), jobScheduler, 1,
                mock( MismatchedStoreIdService.class ), NullLogProvider.getInstance() );

        RaftState raftState = RaftStateBuilder.raftState()
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
        MembershipWaiter waiter = new MembershipWaiter( member( 0 ), jobScheduler, 1,
                mock( MismatchedStoreIdService.class ), NullLogProvider.getInstance() );

        RaftState raftState = RaftStateBuilder.raftState()
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

}
