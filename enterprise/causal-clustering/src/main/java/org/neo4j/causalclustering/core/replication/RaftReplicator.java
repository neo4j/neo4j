/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.core.replication;

import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.core.consensus.LeaderListener;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import org.neo4j.causalclustering.core.replication.session.OperationContext;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * A replicator implementation suitable in a RAFT context. Will handle resending due to timeouts and leader switches.
 */
public class RaftReplicator extends LifecycleAdapter implements Replicator, LeaderListener
{
    private final MemberId me;
    private final Outbound<MemberId,RaftMessages.RaftMessage> outbound;
    private final ProgressTracker progressTracker;
    private final LocalSessionPool sessionPool;
    private final TimeoutStrategy progressTimeoutStrategy;
    private final AvailabilityGuard availabilityGuard;
    private final LeaderLocator leaderLocator;
    private final TimeoutStrategy leaderTimeoutStrategy;
    private final Log log;
    private final Throttler throttler;
    private final long availabilityTimeoutMillis;

    public RaftReplicator( LeaderLocator leaderLocator, MemberId me, Outbound<MemberId,RaftMessages.RaftMessage> outbound, LocalSessionPool sessionPool,
            ProgressTracker progressTracker, TimeoutStrategy progressTimeoutStrategy, TimeoutStrategy leaderTimeoutStrategy,
            long availabilityTimeoutMillis, AvailabilityGuard availabilityGuard, LogProvider logProvider, long replicationLimit )
    {
        this.me = me;
        this.outbound = outbound;
        this.progressTracker = progressTracker;
        this.sessionPool = sessionPool;
        this.progressTimeoutStrategy = progressTimeoutStrategy;
        this.leaderTimeoutStrategy = leaderTimeoutStrategy;
        this.availabilityTimeoutMillis = availabilityTimeoutMillis;
        this.availabilityGuard = availabilityGuard;
        this.throttler = new Throttler( replicationLimit );
        this.leaderLocator = leaderLocator;
        leaderLocator.registerListener( this );
        log = logProvider.getLog( getClass() );
    }

    @Override
    public Future<Object> replicate( ReplicatedContent command, boolean trackResult ) throws ReplicationFailureException
    {
        MemberId originalLeader;
        try
        {
            originalLeader = leaderLocator.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            throw new ReplicationFailureException( "Replication aborted since no leader was available", e );
        }

        if ( command.hasSize() )
        {
            try
            {
                return throttler.invoke( () -> replicate0( command, trackResult, originalLeader ), command.size() );
            }
            catch ( InterruptedException e )
            {
                throw new ReplicationFailureException( "Interrupted while waiting for replication credits", e );
            }
        }
        else
        {
            return replicate0( command, trackResult, originalLeader );
        }
    }

    private Future<Object> replicate0( ReplicatedContent command, boolean trackResult, MemberId leader ) throws ReplicationFailureException
    {
        assertNoLeaderSwitch( leader );

        OperationContext session = sessionPool.acquireSession();

        DistributedOperation operation = new DistributedOperation( command, session.globalSession(), session.localOperationId() );
        Progress progress = progressTracker.start( operation );

        TimeoutStrategy.Timeout progressTimeout = progressTimeoutStrategy.newTimeout();
        TimeoutStrategy.Timeout leaderTimeout = leaderTimeoutStrategy.newTimeout();
        try
        {
            do
            {
                assertDatabaseAvailable();
                try
                {
                    // blocking at least until the send has succeeded or failed before retrying
                    outbound.send( leader, new RaftMessages.NewEntry.Request( me, operation ), true );

                    leaderTimeout = leaderTimeoutStrategy.newTimeout();

                    progress.awaitReplication( progressTimeout.getMillis() );
                    progressTimeout.increment();
                    leader = leaderLocator.getLeader();
                }
                catch ( NoLeaderFoundException e )
                {
                    log.debug( "Could not replicate operation " + operation + " because no leader was found. Retrying.", e );
                    Thread.sleep( leaderTimeout.getMillis() );
                    leaderTimeout.increment();
                }
            }
            while ( !progress.isReplicated() );
        }
        catch ( InterruptedException e )
        {
            progressTracker.abort( operation );
            throw new ReplicationFailureException( "Interrupted while replicating", e );
        }

        BiConsumer<Object,Throwable> cleanup = ( ignored1, ignored2 ) -> sessionPool.releaseSession( session );

        if ( trackResult )
        {
            progress.futureResult().whenComplete( cleanup );
        }
        else
        {
            cleanup.accept( null, null );
        }

        return progress.futureResult();
    }

    private void assertNoLeaderSwitch( MemberId originalLeader ) throws ReplicationFailureException
    {
        MemberId currentLeader;
        try
        {
            currentLeader = leaderLocator.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            throw new ReplicationFailureException( "Replication aborted since no leader was available", e );
        }

        if ( !currentLeader.equals( originalLeader ) )
        {
            throw new ReplicationFailureException( "Replication aborted since a leader switch was detected" );
        }
    }

    @Override
    public void onLeaderSwitch( LeaderInfo leaderInfo )
    {
        progressTracker.triggerReplicationEvent();
    }

    private void assertDatabaseAvailable() throws ReplicationFailureException
    {
        try
        {
            availabilityGuard.await( availabilityTimeoutMillis );
        }
        catch ( AvailabilityGuard.UnavailableException e )
        {
            throw new ReplicationFailureException( "Database is not available, transaction cannot be replicated.", e );
        }
    }
}
