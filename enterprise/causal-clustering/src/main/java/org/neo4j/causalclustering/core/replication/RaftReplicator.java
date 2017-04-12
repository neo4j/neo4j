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
package org.neo4j.causalclustering.core.replication;

import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import org.neo4j.causalclustering.core.replication.session.OperationContext;
import org.neo4j.causalclustering.helper.RetryStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * A replicator implementation suitable in a RAFT context. Will handle resending due to timeouts and leader switches.
 */
public class RaftReplicator extends LifecycleAdapter implements Replicator, Listener<MemberId>
{
    private final MemberId me;
    private final Outbound<MemberId,RaftMessages.RaftMessage> outbound;
    private final ProgressTracker progressTracker;
    private final LocalSessionPool sessionPool;
    private final RetryStrategy retryStrategy;
    private final AvailabilityGuard availabilityGuard;
    private final LeaderLocator leaderLocator;
    private final Log log;

    public RaftReplicator( LeaderLocator leaderLocator, MemberId me,
            Outbound<MemberId,RaftMessages.RaftMessage> outbound, LocalSessionPool sessionPool,
            ProgressTracker progressTracker, RetryStrategy retryStrategy, AvailabilityGuard availabilityGuard,
            LogProvider logProvider )
    {
        this.me = me;
        this.outbound = outbound;
        this.progressTracker = progressTracker;
        this.sessionPool = sessionPool;
        this.retryStrategy = retryStrategy;
        this.availabilityGuard = availabilityGuard;

        this.leaderLocator = leaderLocator;
        leaderLocator.registerListener( this );
        log = logProvider.getLog( getClass() );
    }

    @Override
    public Future<Object> replicate( ReplicatedContent command, boolean trackResult ) throws InterruptedException, NoLeaderFoundException
    {
        OperationContext session = sessionPool.acquireSession();

        DistributedOperation operation = new DistributedOperation( command, session.globalSession(), session.localOperationId() );
        Progress progress = progressTracker.start( operation );

        RetryStrategy.Timeout timeout = retryStrategy.newTimeout();
        do
        {
            assertDatabaseNotShutdown();
            try
            {
                outbound.send( leaderLocator.getLeader(), new RaftMessages.NewEntry.Request( me, operation ) );
                progress.awaitReplication( timeout.getMillis() );
                timeout.increment();
            }
            catch ( InterruptedException e )
            {
                progressTracker.abort( operation );
                throw e;
            }
            catch ( NoLeaderFoundException e )
            {
                log.debug( "Could not replicate operation " + operation + " because no leader was found. Retrying.", e );
            }
        }
        while ( !progress.isReplicated() );

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

    @Override
    public void receive( MemberId leader )
    {
        progressTracker.triggerReplicationEvent();
    }

    private void assertDatabaseNotShutdown() throws InterruptedException
    {
        if ( availabilityGuard.isShutdown() )
        {
            throw new DatabaseShutdownException( "Database has been shutdown, transaction cannot be replicated." );
        }
    }
}
