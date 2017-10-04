/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.membership;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.state.ExposedRaftState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Waits until member has "fully joined" the raft membership.
 * We consider a member fully joined where:
 * <ul>
 * <li>It is a member of the voting group
 * (its opinion will count towards leader elections and committing entries), and</li>
 * <li>It is sufficiently caught up with the leader,
 * so that long periods of unavailability are unlikely, should the leader fail.</li>
 * </ul>
 * <p>
 * To determine whether the member is sufficiently caught up, we check periodically how far behind we are,
 * once every {@code maxCatchupLag}. If the leader is always moving forwards we will never fully catch up,
 * so all we look for is that we have caught up with where the leader was the <i>previous</i> time
 * that we checked.
 */
public class MembershipWaiter
{
    private final MemberId myself;
    private final JobScheduler jobScheduler;
    private final Supplier<DatabaseHealth> dbHealthSupplier;
    private final long maxCatchupLag;
    private long currentCatchupDelayInMs;
    private final Log log;

    public MembershipWaiter( MemberId myself, JobScheduler jobScheduler, Supplier<DatabaseHealth> dbHealthSupplier,
            long maxCatchupLag, LogProvider logProvider )
    {
        this.myself = myself;
        this.jobScheduler = jobScheduler;
        this.dbHealthSupplier = dbHealthSupplier;
        this.maxCatchupLag = maxCatchupLag;
        this.currentCatchupDelayInMs = maxCatchupLag;
        this.log = logProvider.getLog( getClass() );
    }

    CompletableFuture<Boolean> waitUntilCaughtUpMember( RaftMachine raft )
    {
        CompletableFuture<Boolean> catchUpFuture = new CompletableFuture<>();

        Evaluator evaluator = new Evaluator( raft, catchUpFuture, dbHealthSupplier );

        JobScheduler.JobHandle jobHandle = jobScheduler.schedule(
                new JobScheduler.Group( getClass().toString() ),
                evaluator, currentCatchupDelayInMs, MILLISECONDS );

        catchUpFuture.whenComplete( ( result, e ) -> jobHandle.cancel( true ) );

        return catchUpFuture;
    }

    private class Evaluator implements Runnable
    {
        private final RaftMachine raft;
        private final CompletableFuture<Boolean> catchUpFuture;

        private long lastLeaderCommit;
        private final Supplier<DatabaseHealth> dbHealthSupplier;

        private Evaluator( RaftMachine raft, CompletableFuture<Boolean> catchUpFuture,
                Supplier<DatabaseHealth> dbHealthSupplier )
        {
            this.raft = raft;
            this.catchUpFuture = catchUpFuture;
            this.lastLeaderCommit = raft.state().leaderCommit();
            this.dbHealthSupplier = dbHealthSupplier;
        }

        @Override
        public void run()
        {
            if ( !dbHealthSupplier.get().isHealthy() )
            {
                catchUpFuture.completeExceptionally( dbHealthSupplier.get().cause() );
            }
            else if ( iAmAVotingMember() && caughtUpWithLeader() )
            {
                catchUpFuture.complete( true );
            }
            else
            {
                currentCatchupDelayInMs += SECONDS.toMillis( 1 );
                long longerDelay = currentCatchupDelayInMs < maxCatchupLag ? currentCatchupDelayInMs : maxCatchupLag;
                jobScheduler.schedule( new JobScheduler.Group( MembershipWaiter.class.toString() ), this,
                        longerDelay, MILLISECONDS );
            }
        }

        private boolean iAmAVotingMember()
        {
            Set votingMembers = raft.state().votingMembers();
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

            ExposedRaftState state = raft.state();
            long localCommit = state.commitIndex();
            lastLeaderCommit = state.leaderCommit();
            if ( lastLeaderCommit != -1 )
            {
                caughtUpWithLeader = localCommit == lastLeaderCommit;
                long gap = lastLeaderCommit - localCommit;
                log.info( "%s Catchup: %d => %d (%d behind)", myself, localCommit, lastLeaderCommit, gap );
            }
            else
            {
                log.info( "Leader commit unknown" );
            }
            return caughtUpWithLeader;
        }
    }

}
