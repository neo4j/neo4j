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

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.neo4j.coreedge.core.consensus.MismatchedStoreIdService;
import org.neo4j.coreedge.core.consensus.state.ReadableRaftState;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.POOLED;

/**
 * Waits until member has "fully joined" the raft membership.
 * We consider a member fully joined where:
 * <ul>
 *     <li>It is a member of the voting group
 *     (its opinion will count towards leader elections and committing entries), and</li>
 *     <li>It is sufficiently caught up with the leader,
 *     so that long periods of unavailability are unlikely, should the leader fail.</li>
 * </ul>
 *
 * To determine whether the member is sufficiently caught up, we check periodically how far behind we are,
 * once every {@code maxCatchupLag}. If the leader is always moving forwards we will never fully catch up,
 * so all we look for is that we have caught up with where the leader was the <i>previous</i> time
 * that we checked.
 */
public class MembershipWaiter
{
    private final MemberId myself;
    private final JobScheduler jobScheduler;
    private final long maxCatchupLag;
    private final MismatchedStoreIdService mismatchedStoreIdService;
    private final Log log;

    public MembershipWaiter( MemberId myself, JobScheduler jobScheduler, long maxCatchupLag,
                             MismatchedStoreIdService mismatchedStoreIdService, LogProvider logProvider )
    {
        this.myself = myself;
        this.jobScheduler = jobScheduler;
        this.maxCatchupLag = maxCatchupLag;
        this.mismatchedStoreIdService = mismatchedStoreIdService;
        this.log = logProvider.getLog( getClass() );
    }

    CompletableFuture<Boolean> waitUntilCaughtUpMember( ReadableRaftState raftState )
    {
        CompletableFuture<Boolean> catchUpFuture = new CompletableFuture<>();

        Evaluator evaluator = new Evaluator( raftState, catchUpFuture );
        mismatchedStoreIdService.addMismatchedStoreListener( evaluator );

        JobScheduler.JobHandle jobHandle = jobScheduler.scheduleRecurring(
                new JobScheduler.Group( getClass().toString(), POOLED ),
                evaluator, maxCatchupLag, MILLISECONDS );

        catchUpFuture.whenComplete( ( result, e ) -> jobHandle.cancel( true ) );

        return catchUpFuture;
    }

    private class Evaluator implements Runnable, MismatchedStoreIdService.MismatchedStoreListener
    {
        private final ReadableRaftState raftState;
        private final CompletableFuture<Boolean> catchUpFuture;

        private long lastLeaderCommit;

        private Evaluator( ReadableRaftState raftState, CompletableFuture<Boolean> catchUpFuture )
        {
            this.raftState = raftState;
            this.catchUpFuture = catchUpFuture;
            this.lastLeaderCommit = raftState.leaderCommit();
        }

        public void run()
        {
            if ( iAmAVotingMember() && caughtUpWithLeader() )
            {
                catchUpFuture.complete( true );
            }
        }

        private boolean iAmAVotingMember()
        {
            Set votingMembers = raftState.votingMembers();
            boolean votingMember = votingMembers.contains( myself );
            if ( !votingMember )
            {
                log.debug( "I (%s) am not a voting member: [%s]", myself, votingMembers );
            }
            return votingMember;
        }

        private boolean caughtUpWithLeader()
        {
            boolean caughtUpWithLeader = false;

            long localCommit = raftState.commitIndex();
            if ( lastLeaderCommit != -1 )
            {
                caughtUpWithLeader = localCommit >= lastLeaderCommit;
            }
            lastLeaderCommit = raftState.leaderCommit();
            if ( lastLeaderCommit != -1 )
            {
                long gap = lastLeaderCommit - localCommit;
                log.info( "%s Catchup: %d => %d (%d behind)", myself, localCommit, lastLeaderCommit, gap );
            }
            else
            {
                log.info( "Leader commit unknown" );
            }

            return caughtUpWithLeader;
        }

        @Override
        public void onMismatchedStore( MismatchedStoreIdService.MismatchedStoreIdException ex )
        {
            catchUpFuture.completeExceptionally( ex );
        }
    }
}
