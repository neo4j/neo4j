/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.concurrent.CompletableFuture;

import org.neo4j.coreedge.raft.state.ReadableRaftState;
import org.neo4j.kernel.impl.util.JobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.POOLED;

public class MembershipWaiter<MEMBER>
{
    private final MEMBER myself;
    private final JobScheduler jobScheduler;
    private final long maxCatchupLag;

    public MembershipWaiter( MEMBER myself, JobScheduler jobScheduler, long maxCatchupLag )
    {
        this.myself = myself;
        this.jobScheduler = jobScheduler;
        this.maxCatchupLag = maxCatchupLag;
    }

    public CompletableFuture<Boolean> waitUntilCaughtUpMember( ReadableRaftState<MEMBER> raftState )
    {
        CompletableFuture<Boolean> catchUpFuture = new CompletableFuture<>();

        JobScheduler.JobHandle jobHandle = jobScheduler.scheduleRecurring(
                new JobScheduler.Group( getClass().toString(), POOLED ),
                new Evaluator<>( raftState, myself, catchUpFuture ), maxCatchupLag, MILLISECONDS );

        catchUpFuture.whenComplete( ( result, e ) -> jobHandle.cancel( true ) );

        return catchUpFuture;
    }

    private static class Evaluator<MEMBER> implements Runnable
    {
        private final ReadableRaftState<MEMBER> raftState;
        private final MEMBER myself;
        private final CompletableFuture<Boolean> catchUpFuture;

        private long lastLeaderCommit;

        private Evaluator( ReadableRaftState<MEMBER> raftState, MEMBER myself, CompletableFuture<Boolean> catchUpFuture )
        {
            this.raftState = raftState;
            this.myself = myself;
            this.catchUpFuture = catchUpFuture;
            this.lastLeaderCommit = raftState.leaderCommit();
        }

        public void run()
        {
            if ( iAmAVotingMember() && caughtUpWithLeader())
            {
                catchUpFuture.complete( true );
            }
        }

        private boolean iAmAVotingMember()
        {
            return raftState.votingMembers().contains( myself );
        }

        private boolean caughtUpWithLeader()
        {
            boolean caughtUpWithLeader = false;

            if ( lastLeaderCommit != -1 )
            {
                caughtUpWithLeader = raftState.entryLog().commitIndex() >= lastLeaderCommit;
            }
            lastLeaderCommit = raftState.leaderCommit();
            System.out.printf( "%s CATCHUP: %d => %d (%d behind)%n",
                    myself,
                    raftState.entryLog().commitIndex(), raftState.leaderCommit(),
                    raftState.leaderCommit() - raftState.entryLog().commitIndex() );

            return caughtUpWithLeader;
        }
    }
}
