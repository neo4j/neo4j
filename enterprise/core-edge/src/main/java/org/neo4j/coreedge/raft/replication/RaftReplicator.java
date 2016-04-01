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
package org.neo4j.coreedge.raft.replication;

import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import org.neo4j.coreedge.raft.LeaderLocator;
import org.neo4j.coreedge.raft.NoLeaderFoundException;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.replication.session.LocalSessionPool;
import org.neo4j.coreedge.raft.replication.session.OperationContext;
import org.neo4j.coreedge.raft.replication.tx.RetryStrategy;
import org.neo4j.kernel.impl.util.Listener;

/**
 * A replicator implementation suitable in a RAFT context. Will handle resending due to timeouts and leader switches.
 */
public class RaftReplicator<MEMBER> implements Replicator, Listener<MEMBER>
{
    private final MEMBER me;
    private final Outbound<MEMBER> outbound;
    private final ProgressTracker progressTracker;
    private final LocalSessionPool sessionPool;
    private final RetryStrategy retryStrategy;

    private MEMBER leader;

    public RaftReplicator( LeaderLocator<MEMBER> leaderLocator, MEMBER me, Outbound<MEMBER> outbound, LocalSessionPool<MEMBER> sessionPool, ProgressTracker progressTracker, RetryStrategy retryStrategy )
    {
        this.me = me;
        this.outbound = outbound;
        this.progressTracker = progressTracker;
        this.sessionPool = sessionPool;
        this.retryStrategy = retryStrategy;

        try
        {
            this.leader = leaderLocator.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            this.leader = null;
        }
        leaderLocator.registerListener( this );
    }

    @Override
    public Future<Object> replicate( ReplicatedContent command, boolean trackResult ) throws InterruptedException
    {
        OperationContext session = sessionPool.acquireSession();

        DistributedOperation operation = new DistributedOperation( command, session.globalSession(), session.localOperationId() );
        Progress progress = progressTracker.start( operation );

        RetryStrategy.Timeout timeout = retryStrategy.newTimeout();
        do
        {
            outbound.send( leader, new RaftMessages.NewEntry.Request<>( me, operation ) );
            try
            {
                progress.awaitReplication( timeout.getMillis() );
                timeout.increment();
            }
            catch ( InterruptedException e )
            {
                progressTracker.abort( operation );
                throw e;
            }
        } while( !progress.isReplicated() );

        BiConsumer<Object,Throwable> cleanup = ( ignored1, ignored2 ) -> {
            sessionPool.releaseSession( session );
        };

        if( trackResult )
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
    public void receive( MEMBER leader )
    {
        this.leader = leader;
        progressTracker.triggerReplicationEvent();
    }
}
