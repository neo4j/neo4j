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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTracker;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.coreedge.server.core.CurrentReplicatedLockState;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;

import static org.neo4j.coreedge.raft.replication.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;

public class ReplicatedTransactionStateMachine implements Replicator.ReplicatedContentListener
{
    private final GlobalSessionTracker sessionTracker;
    private final GlobalSession myGlobalSession;
    private final CurrentReplicatedLockState currentReplicatedLockState;
    private final TransactionCommitProcess commitProcess;
    private final Map<LocalOperationId, FutureTxId> outstanding = new ConcurrentHashMap<>();
    private long lastCommittedIndex = -1;

    public ReplicatedTransactionStateMachine( TransactionCommitProcess commitProcess,
                                              GlobalSession myGlobalSession,
                                              CurrentReplicatedLockState currentReplicatedLockState )
    {
        this.commitProcess = commitProcess;
        this.myGlobalSession = myGlobalSession;
        this.currentReplicatedLockState = currentReplicatedLockState;
        this.sessionTracker = new GlobalSessionTracker();
    }

    public Future<Long> getFutureTxId( LocalOperationId localOperationId )
    {
        final FutureTxId future = new FutureTxId( localOperationId );
        outstanding.put( localOperationId, future );
        return future;
    }

    @Override
    public synchronized void onReplicated( ReplicatedContent content, long logIndex )
    {
        if ( content instanceof ReplicatedTransaction )
        {
            handleTransaction( (ReplicatedTransaction) content, logIndex );
        }
    }

    private void handleTransaction( ReplicatedTransaction replicatedTx, long logIndex )
    {
        if ( !sessionTracker.validateAndTrackOperation( replicatedTx.globalSession(), replicatedTx.localOperationId() )
                || logIndex <= lastCommittedIndex )
        {
            return;
        }

        try
        {
            byte[] extraHeader = encodeLogIndexAsTxHeader( logIndex );
            TransactionRepresentation tx = ReplicatedTransactionFactory.extractTransactionRepresentation(
                    replicatedTx, extraHeader );

            // A missing future means the transaction does not belong to this instance
            Optional<CompletableFuture<Long>> future = replicatedTx.globalSession().equals( myGlobalSession ) ?
                    Optional.ofNullable( outstanding.remove( replicatedTx.localOperationId() ) ) :
                    Optional.<CompletableFuture<Long>>empty();

            if ( currentReplicatedLockState.currentLockSession().id() != tx.getLockSessionId() )
            {
                future.ifPresent( txFuture -> txFuture.completeExceptionally( new TransientTransactionFailureException(
                        "Attempt to commit transaction that was started on a different leader. " +
                                "Please retry the transaction." ) ) );
                return;
            }

            try
            {
               long txId = commitProcess.commit( new TransactionToApply( tx ), CommitEvent.NULL,
                        TransactionApplicationMode.EXTERNAL );
                future.ifPresent( txFuture -> txFuture.complete( txId ) );
            }
            catch ( TransientFailureException e )
            {
                future.ifPresent( txFuture -> txFuture.completeExceptionally( e ) );
            }
        }
        catch ( TransactionFailureException | IOException e )
        {
            throw new IllegalStateException( "Failed to locally commit a transaction that has already been " +
                    "committed to the RAFT log. This server cannot process later transactions and needs to be " +
                    "restarted once the underlying cause has been addressed.", e );
        }
    }

    public void setLastCommittedIndex( long lastCommittedIndex )
    {
        this.lastCommittedIndex = lastCommittedIndex;
    }

    private class FutureTxId extends CompletableFuture<Long>
    {
        private final LocalOperationId localOperationId;

        FutureTxId( LocalOperationId localOperationId )
        {
            this.localOperationId = localOperationId;
        }

        @Override
        public boolean cancel( boolean mayInterruptIfRunning )
        {
            if ( !super.cancel( mayInterruptIfRunning ) )
            {
                return false;
            }
            outstanding.remove( localOperationId );
            return true;
        }
    }

}
