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
import java.util.Optional;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTracker;
import org.neo4j.coreedge.server.core.locks.LockTokenManager;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.coreedge.raft.replication.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LockSessionInvalid;

public class ReplicatedTransactionStateMachine implements Replicator.ReplicatedContentListener
{
    private final GlobalSessionTracker sessionTracker;
    private final GlobalSession myGlobalSession;
    private final LockTokenManager lockTokenManager;
    private final TransactionCommitProcess commitProcess;
    private final CommittingTransactions transactionFutures;
    private long lastCommittedIndex = -1;

    public ReplicatedTransactionStateMachine( TransactionCommitProcess commitProcess,
                                              GlobalSession myGlobalSession,
                                              LockTokenManager lockTokenManager,
                                              CommittingTransactions transactionFutures )
    {
        this.commitProcess = commitProcess;
        this.myGlobalSession = myGlobalSession;
        this.lockTokenManager = lockTokenManager;
        this.transactionFutures = transactionFutures;
        this.sessionTracker = new GlobalSessionTracker();
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

        TransactionRepresentation tx;
        try
        {
            byte[] extraHeader = encodeLogIndexAsTxHeader( logIndex );
            tx = ReplicatedTransactionFactory.extractTransactionRepresentation(
                    replicatedTx, extraHeader );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Failed to locally commit a transaction that has already been " +
                    "committed to the RAFT log. This server cannot process later transactions and needs to be " +
                    "restarted once the underlying cause has been addressed.", e );
        }

        // A missing future means the transaction does not belong to this instance
        Optional<CommittingTransaction> future = replicatedTx.globalSession().equals( myGlobalSession ) ?
                Optional.ofNullable( transactionFutures.retrieve( replicatedTx.localOperationId() ) ) :
                Optional.<CommittingTransaction>empty();

        int currentTokenId = lockTokenManager.currentToken().id();
        int txLockSessionId = tx.getLockSessionId();

        if ( currentTokenId != txLockSessionId && txLockSessionId != Locks.Client.NO_LOCK_SESSION_ID )
        {
            future.ifPresent( txFuture -> txFuture.notifyCommitFailed( new TransactionFailureException( LockSessionInvalid,
                    "The lock session in the cluster has changed: " +
                            "[current lock session id:%d, tx lock session id:%d]",
                    currentTokenId, txLockSessionId ) ) );
            return;
        }

        try
        {
           long txId = commitProcess.commit( new TransactionToApply( tx ), CommitEvent.NULL,
                   TransactionApplicationMode.EXTERNAL );

            future.ifPresent( txFuture -> txFuture.notifySuccessfullyCommitted( txId ) );
        }
        catch ( TransactionFailureException e )
        {
            future.ifPresent( txFuture -> txFuture.notifyCommitFailed( e ) );
            throw new IllegalStateException( "Failed to locally commit a transaction that has already been " +
                    "committed to the RAFT log. This server cannot process later transactions and needs to be " +
                    "restarted once the underlying cause has been addressed.", e );
        }
    }

    public void setLastCommittedIndex( long lastCommittedIndex )
    {
        this.lastCommittedIndex = lastCommittedIndex;
    }
}
