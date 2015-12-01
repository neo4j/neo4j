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
package org.neo4j.coreedge.raft.replication.tx;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.replication.Replicator.ReplicationFailedException;
import org.neo4j.coreedge.raft.replication.session.LocalSessionPool;
import org.neo4j.coreedge.raft.replication.session.OperationContext;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionFactory.createImmutableReplicatedTransaction;

public class ReplicatedTransactionCommitProcess extends LifecycleAdapter implements TransactionCommitProcess
{
    private final Replicator replicator;
    private final ReplicatedTransactionStateMachine replicatedTxListener;
    private final Clock clock;
    private final long retryIntervalMillis;
    private final long maxRetryTimeMillis;
    private final LocalSessionPool sessionPool;

    public ReplicatedTransactionCommitProcess( Replicator replicator, LocalSessionPool sessionPool,
            ReplicatedTransactionStateMachine replicatedTxListener, Clock clock,
            long retryIntervalMillis, long maxRetryTimeMillis )
    {
        this.sessionPool = sessionPool;
        this.replicatedTxListener = replicatedTxListener;
        this.replicator = replicator;
        this.clock = clock;
        this.retryIntervalMillis = retryIntervalMillis;
        this.maxRetryTimeMillis = maxRetryTimeMillis;
        replicator.subscribe( this.replicatedTxListener );
    }

    @Override
    public long commit( final TransactionToApply tx,
                        final CommitEvent commitEvent,
                        TransactionApplicationMode mode ) throws TransactionFailureException
    {
        OperationContext operationContext = sessionPool.acquireSession();

        ReplicatedTransaction transaction;
        try
        {
            transaction = createImmutableReplicatedTransaction( tx.transactionRepresentation(),
                    operationContext.globalSession(), operationContext.localOperationId() );
        }
        catch ( IOException e )
        {
            throw new TransactionFailureException( "Could not create immutable object for replication", e );
        }

        boolean lastRound = false;
        long startTime = clock.currentTimeMillis();
        while ( true )
        {
            final Future<Long> futureTxId = replicatedTxListener.getFutureTxId( operationContext.localOperationId() );
            try
            {
                replicator.replicate( transaction );

                /* The last round should wait for a longer time to keep issues arising from false negatives very rare
                *  (e.g. local actor thinks commit failed, while it was committed in the cluster). */
                long responseWaitTime = lastRound ? retryIntervalMillis : maxRetryTimeMillis/2;
                Long txId = futureTxId.get( responseWaitTime, TimeUnit.MILLISECONDS );

                sessionPool.releaseSession( operationContext );

                return txId;
            }
            catch ( InterruptedException | TimeoutException  e )
            {
                futureTxId.cancel( false );

                if ( lastRound )
                {
                    throw new TransactionFailureException( "Failed to commit transaction within time bound", e );
                }
                else if ( (clock.currentTimeMillis() - startTime) >= maxRetryTimeMillis/2 )
                {
                    lastRound = true;
                }
            }
            catch ( ReplicationFailedException | ExecutionException e )
            {
                throw new TransactionFailureException( "Failed to replicate transaction", e );
            }
            System.out.println( "Retrying replication" );
        }
    }

    @Override
    public void stop()
    {
        replicator.unsubscribe( replicatedTxListener );
    }
}
