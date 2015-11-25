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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.neo4j.concurrent.CompletableFuture;
import org.neo4j.coreedge.raft.locks.CoreServiceAssignment;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTracker;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;

public class ReplicatedTransactionStateMachine implements Replicator.ReplicatedContentListener
{
    private final TransactionCommitProcess localCommitProcess;
    private final GlobalSessionTracker sessionTracker;
    private final GlobalSession myGlobalSession;

    private final Map<LocalOperationId, FutureTxId> outstanding = new ConcurrentHashMap<>();
    private long lastCommittedTxId; // Maintains the last committed tx id, used to set the next field
    private long lastTxIdForPreviousAssignment; // Maintains the last txid committed under the previous service assignment

    public ReplicatedTransactionStateMachine( TransactionCommitProcess localCommitProcess, GlobalSessionTracker
            sessionTracker, GlobalSession myGlobalSession )
    {
        this.localCommitProcess = localCommitProcess;
        this.sessionTracker = sessionTracker;
        this.myGlobalSession = myGlobalSession;
    }

    public Future<Long> getFutureTxId( LocalOperationId localOperationId )
    {
        final FutureTxId future = new FutureTxId( localOperationId );
        outstanding.put( localOperationId, future );
        return future;
    }

    @Override
    public void onReplicated( ReplicatedContent content )
    {
        if ( content instanceof ReplicatedTransaction )
        {
            handleTransaction( (ReplicatedTransaction) content );
        }
        else if ( content instanceof CoreServiceAssignment )
        {
            // This essentially signifies a leader switch. We should properly name the content class
            lastTxIdForPreviousAssignment = lastCommittedTxId;
        }
    }

    private void handleTransaction( ReplicatedTransaction replicatedTransaction )
    {
        try
        {
            synchronized ( this )
            {

                if ( sessionTracker.validateAndTrackOperation( replicatedTransaction.globalSession(),
                        replicatedTransaction.localOperationId() ) )
                {
                    TransactionRepresentation tx = ReplicatedTransactionFactory.extractTransactionRepresentation(
                            replicatedTransaction );
                    boolean shouldReject = false;
                    if ( tx.getLatestCommittedTxWhenStarted() < lastTxIdForPreviousAssignment )
                    {
                        // This means the transaction started before the last transaction for the previous term. Reject.
                        shouldReject = true;
                    }

                    long txId = -1;
                    if ( !shouldReject )
                    {
                        try ( LockGroup lockGroup = new LockGroup() )
                        {
                            txId = localCommitProcess.commit( tx, lockGroup, CommitEvent.NULL,
                                    TransactionApplicationMode.EXTERNAL );
                            lastCommittedTxId = txId;
                        }
                    }

                    if ( replicatedTransaction.globalSession().equals( myGlobalSession ) )
                    {
                        CompletableFuture<Long> future = outstanding.remove( replicatedTransaction.localOperationId() );
                        if ( future != null )
                        {
                            if ( shouldReject )
                            {
                                future.completeExceptionally( new TransientTransactionFailureException(
                                        "Attempt to commit transaction that was started on a different leader term. " +
                                                "Please retry the transaction." ) );
                            }
                            else
                            {
                                future.complete( txId );
                            }
                        }
                    }
                }

            }
        }
        catch ( TransactionFailureException | IOException e )
        {
            throw new IllegalStateException( "Failed to locally commit a transaction that has already been " +
                    "committed to the RAFT log. This server cannot process later transactions and needs to be " +
                    "restarted once the underlying cause has been addressed.", e );
        }
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
