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
package org.neo4j.coreedge.raft.replication.tx;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.replication.Replicator.ReplicationFailedException;
import org.neo4j.coreedge.raft.replication.session.LocalSessionPool;
import org.neo4j.coreedge.raft.replication.session.OperationContext;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionFactory.createImmutableReplicatedTransaction;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.CouldNotCommit;

public class ReplicatedTransactionCommitProcess extends LifecycleAdapter implements TransactionCommitProcess
{
    private final Replicator replicator;
    private final Replicator.ReplicatedContentListener replicatedTxListener;
    private final long retryIntervalMillis;
    private final LocalSessionPool sessionPool;
    private final Log log;
    private final CommittingTransactions txFutures;

    public ReplicatedTransactionCommitProcess( Replicator replicator, LocalSessionPool sessionPool,
                                               Replicator.ReplicatedContentListener replicatedTxListener,
                                               long retryIntervalMillis, LogService logging,
                                               CommittingTransactions txFutures )
    {
        this.sessionPool = sessionPool;
        this.replicatedTxListener = replicatedTxListener;
        this.replicator = replicator;
        this.retryIntervalMillis = retryIntervalMillis;
        this.log = logging.getInternalLog( getClass() );
        this.txFutures = txFutures;
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
            throw new TransactionFailureException( "Could not create immutable transaction for replication", e );
        }

        boolean hasNeverReplicated = true;
        boolean interrupted = false;
        try ( CommittingTransaction futureTxId = txFutures.register( operationContext.localOperationId() ) )
        {
            while ( true )
            {
                try
                {
                    replicator.replicate( transaction );
                    hasNeverReplicated = false;
                }
                catch ( ReplicationFailedException e )
                {
                    if ( hasNeverReplicated )
                    {
                        throw new TransactionFailureException( CouldNotCommit, "Failed to replicate transaction", e );
                    }
                    log.warn( "Transaction replication failed, but a previous attempt may have succeeded," +
                            "so commit process must keep waiting for possible success." );
                }

                try
                {
                    Long txId = futureTxId.waitUntilCommitted( retryIntervalMillis, TimeUnit.MILLISECONDS );
                    sessionPool.releaseSession( operationContext );

                    return txId;
                }
                catch ( InterruptedException e )
                {
                    interrupted = true;
                    log.info( "Replication of %s was interrupted; retrying.", operationContext );
                }
                catch ( TimeoutException e )
                {
                    log.info( "Replication of %s timed out after %d %s; retrying.",
                            operationContext, retryIntervalMillis, TimeUnit.MILLISECONDS );
                }
            }
        }
        finally
        {
            if ( interrupted )
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void stop()
    {
        replicator.unsubscribe( replicatedTxListener );
    }
}
