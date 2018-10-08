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

import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.core.consensus.LeaderListener;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.replication.monitoring.ReplicationMonitor;
import org.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import org.neo4j.causalclustering.core.replication.session.OperationContext;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * A replicator implementation suitable in a RAFT context. Will handle resending due to timeouts and leader switches.
 */
public class RaftReplicator implements Replicator, LeaderListener
{
    private final MemberId me;
    private final Outbound<MemberId,RaftMessages.RaftMessage> outbound;
    private final ProgressTracker progressTracker;
    private final LocalSessionPool sessionPool;
    private final TimeoutStrategy progressTimeoutStrategy;
    private final AvailabilityGuard availabilityGuard;
    private final Log log;
    private final LocalDatabase localDatabase;
    private final ReplicationMonitor replicationMonitor;
    private final long availabilityTimeoutMillis;
    private final LeaderProvider leaderProvider;

    public RaftReplicator( LeaderLocator leaderLocator, MemberId me, Outbound<MemberId,RaftMessages.RaftMessage> outbound, LocalSessionPool sessionPool,
            ProgressTracker progressTracker, TimeoutStrategy progressTimeoutStrategy, long availabilityTimeoutMillis, AvailabilityGuard availabilityGuard,
            LogProvider logProvider, LocalDatabase localDatabase, Monitors monitors )
    {
        this.me = me;
        this.outbound = outbound;
        this.progressTracker = progressTracker;
        this.sessionPool = sessionPool;
        this.progressTimeoutStrategy = progressTimeoutStrategy;
        this.availabilityTimeoutMillis = availabilityTimeoutMillis;
        this.availabilityGuard = availabilityGuard;
        this.log = logProvider.getLog( getClass() );
        this.localDatabase = localDatabase;
        this.replicationMonitor = monitors.newMonitor( ReplicationMonitor.class );
        this.leaderProvider = new LeaderProvider();
        leaderLocator.registerListener( this );
    }

    @Override
    public Future<Object> replicate( ReplicatedContent command, boolean trackResult ) throws ReplicationFailureException
    {
        MemberId currentLeader = leaderProvider.currentLeader();
        if ( currentLeader == null )
        {
            throw new ReplicationFailureException( "Replication aborted since no leader was available" );
        }
        return replicate0( command, trackResult, currentLeader );
    }

    private Future<Object> replicate0( ReplicatedContent command, boolean trackResult, MemberId leader ) throws ReplicationFailureException
    {
        replicationMonitor.startReplication();
        try
        {
            OperationContext session = sessionPool.acquireSession();

            DistributedOperation operation = new DistributedOperation( command, session.globalSession(), session.localOperationId() );
            Progress progress = progressTracker.start( operation );

            TimeoutStrategy.Timeout progressTimeout = progressTimeoutStrategy.newTimeout();
            int attempts = 0;
            try
            {
                while ( true )
                {
                    attempts++;
                    if ( attempts > 1 )
                    {
                        log.info( "Retrying replication. Current attempt: %d Content: %s", attempts, command );
                    }
                    replicationMonitor.replicationAttempt();
                    assertDatabaseAvailable();
                    // blocking at least until the send has succeeded or failed before retrying
                    outbound.send( leader, new RaftMessages.NewEntry.Request( me, operation ), true );
                    progress.awaitReplication( progressTimeout.getMillis() );
                    if ( progress.isReplicated() )
                    {
                        break;
                    }
                    progressTimeout.increment();
                    leader = leaderProvider.awaitLeader();
                }
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
            replicationMonitor.successfulReplication();
            return progress.futureResult();
        }
        catch ( Throwable t )
        {
            replicationMonitor.failedReplication( t );
            throw t;
        }

    }

    @Override
    public void onLeaderSwitch( LeaderInfo leaderInfo )
    {
        progressTracker.triggerReplicationEvent();
        MemberId newLeader = leaderInfo.memberId();
        MemberId oldLeader = leaderProvider.currentLeader();
        if ( newLeader == null && oldLeader != null )
        {
            log.info( "Lost previous leader '%s'. Currently no available leader", oldLeader );
        }
        else if ( newLeader != null && oldLeader == null )
        {
            log.info( "A new leader has been detected: '%s'", newLeader );
        }
        leaderProvider.setLeader( newLeader );
    }

    private void assertDatabaseAvailable() throws ReplicationFailureException
    {
        localDatabase.assertHealthy( ReplicationFailureException.class );
        try
        {
            availabilityGuard.await( availabilityTimeoutMillis );
        }
        catch ( UnavailableException e )
        {
            throw new ReplicationFailureException( "Database is not available, transaction cannot be replicated.", e );
        }
    }
}
