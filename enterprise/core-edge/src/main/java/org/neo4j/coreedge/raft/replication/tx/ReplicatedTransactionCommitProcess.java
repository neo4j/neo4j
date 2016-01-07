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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.replication.Replicator.ReplicationFailedException;
import org.neo4j.coreedge.raft.replication.session.LocalSessionPool;
import org.neo4j.coreedge.raft.replication.session.OperationContext;
import org.neo4j.coreedge.server.core.CurrentReplicatedLockState;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionFactory.createImmutableReplicatedTransaction;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LockSessionInvalid;

public class ReplicatedTransactionCommitProcess extends LifecycleAdapter implements TransactionCommitProcess
{
    private final Replicator replicator;
    private final ReplicatedTransactionStateMachine replicatedTxListener;
    private final long retryIntervalMillis;
    private final CurrentReplicatedLockState currentReplicatedLockState;
    private final LocalSessionPool sessionPool;
    private final Log log;

    public ReplicatedTransactionCommitProcess( Replicator replicator, LocalSessionPool sessionPool,
            ReplicatedTransactionStateMachine replicatedTxListener,
            long retryIntervalMillis, CurrentReplicatedLockState currentReplicatedLockState, LogService logging )
    {
        this.sessionPool = sessionPool;
        this.replicatedTxListener = replicatedTxListener;
        this.replicator = replicator;
        this.retryIntervalMillis = retryIntervalMillis;
        this.currentReplicatedLockState = currentReplicatedLockState;
        this.log = logging.getInternalLog( getClass() );
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

        while ( true )
        {
            final Future<Long> futureTxId = replicatedTxListener.getFutureTxId( operationContext.localOperationId() );
            try
            {
                int currentLockSessionId = currentReplicatedLockState.currentLockSession().id();
                int txLockSessionId = tx.transactionRepresentation().getLockSessionId();
                if ( currentLockSessionId != txLockSessionId )
                {
                    /* It is safe and necessary to give up at this point, since the currently valid lock
                       session of the cluster has changed, and even if a previous replication of the
                       transaction content does eventually get replicated (e.g. delayed on the network),
                       then it will be ignored by the RTSM. So giving up and subsequently releasing
                       locks (in KTI) is safe. */

                    throw new TransactionFailureException( LockSessionInvalid,
                            "The lock session in the cluster has changed: " +
                            "[current lock session id:%d, tx lock session id:%d]",
                            currentLockSessionId, txLockSessionId );
                }

                replicator.replicate( transaction );
                Long txId = futureTxId.get( retryIntervalMillis, TimeUnit.MILLISECONDS );
                sessionPool.releaseSession( operationContext );

                return txId;
            }
            catch ( InterruptedException | TimeoutException  e )
            {
                futureTxId.cancel( false );
            }
            catch ( ReplicationFailedException | ExecutionException e )
            {
                futureTxId.cancel( false );
                throw new TransactionFailureException( "Failed to replicate transaction", e );
            }

            log.info( "Retrying replication: " + operationContext );
        }
    }

    @Override
    public void stop()
    {
        replicator.unsubscribe( replicatedTxListener );
    }
}
