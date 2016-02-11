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
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTrackerState;
import org.neo4j.coreedge.raft.state.StateMachine;
import org.neo4j.coreedge.raft.state.StateStorage;
import org.neo4j.coreedge.server.core.locks.LockTokenManager;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static java.lang.String.format;

import static org.neo4j.coreedge.raft.replication.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LockSessionInvalid;

public class ReplicatedTransactionStateMachine<MEMBER> implements StateMachine
{
    private final GlobalSessionTrackerState<MEMBER> sessionTracker;
    private final GlobalSession myGlobalSession;
    private final LockTokenManager lockTokenManager;
    private final TransactionCommitProcess commitProcess;
    private final CommittingTransactions transactionFutures;
    private final StateStorage<GlobalSessionTrackerState<MEMBER>> storage;
    private final Log log;

    private long lastCommittedIndex = -1;

    public ReplicatedTransactionStateMachine( TransactionCommitProcess commitProcess,
                                              GlobalSession myGlobalSession,
                                              LockTokenManager lockTokenManager,
                                              CommittingTransactions transactionFutures,
                                              StateStorage<GlobalSessionTrackerState<MEMBER>> storage,
                                              LogProvider logProvider )
    {
        this.commitProcess = commitProcess;
        this.myGlobalSession = myGlobalSession;
        this.lockTokenManager = lockTokenManager;
        this.transactionFutures = transactionFutures;
        this.storage = storage;
        this.sessionTracker = storage.getInitialState();
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public synchronized void applyCommand( ReplicatedContent content, long logIndex )
    {
        if ( content instanceof ReplicatedTransaction )
        {
            handleTransaction( (ReplicatedTransaction<MEMBER>) content, logIndex );
        }
    }

    @Override
    public void flush() throws IOException
    {
        storage.persistStoreData( sessionTracker );
    }

    private void handleTransaction( ReplicatedTransaction<MEMBER> replicatedTx, long logIndex )
    {
        /*
         * This check quickly verifies that the session is invalid. Since we update the session state *after* appending
         * the tx to the log, we are certain here that on replay, if the session tracker says that the session is
         * invalid,
         * then the transaction either should never be committed or has already been appended in the log.
         */
        if ( !operationValid( replicatedTx ) )
        {
            log.info( format( "[%d] Invalid operation: %s %s",
                    logIndex, replicatedTx.globalSession(), replicatedTx.localOperationId() ) );
            return;
        }

        // for debugging purposes, we don't really need these
        boolean committed = false;
        boolean sessionUpdated = false;

        try
        {
        /*
         * At this point, we need to check if the tx exists in the log. If it does, it is ok to skip it. However, we
         * may still need to persist the session state (as we may crashed in between), which happens outside this
         * if check.
         */
            if ( logIndex <= lastCommittedIndex )
            {
                log.info( "Ignoring transaction at log index %d since already committed up to %d", logIndex,
                        lastCommittedIndex );
            }
            else
            {
                TransactionRepresentation tx;
                try
                {
                    byte[] extraHeader = encodeLogIndexAsTxHeader( logIndex );
                    tx = ReplicatedTransactionFactory.extractTransactionRepresentation(
                            replicatedTx, extraHeader );
                }
                catch ( IOException e )
                {
                    log.info( format( "[%d] Failed to read transaction representation: %s %s",
                            logIndex, replicatedTx.globalSession(), replicatedTx.localOperationId() ) );

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
                    log.info( format( "[%d] Lock session changed: %s %s",
                            logIndex, replicatedTx.globalSession(), replicatedTx.localOperationId() ) );
                    future.ifPresent( txFuture -> txFuture.notifyCommitFailed( new TransactionFailureException(
                            LockSessionInvalid,
                            "The lock session in the cluster has changed: " +
                                    "[current lock session id:%d, tx lock session id:%d]",
                            currentTokenId, txLockSessionId ) ) );
                    return;
                }

                try
                {
                    long txId = commitProcess.commit( new TransactionToApply( tx ), CommitEvent.NULL,
                            TransactionApplicationMode.EXTERNAL );
                    committed = true;
                    future.ifPresent( txFuture -> txFuture.notifySuccessfullyCommitted( txId ) );
                }
                catch ( TransactionFailureException e )
                {
                    log.info( format( "[%d] Failed to commit transaction: %s %s",
                            logIndex, replicatedTx.globalSession(), replicatedTx.localOperationId() ) );
                    future.ifPresent( txFuture -> txFuture.notifyCommitFailed( e ) );
                    throw new IllegalStateException( "Failed to locally commit a transaction that has already been " +
                            "committed to the RAFT log. This server cannot process later transactions and needs to be" +
                            " restarted once the underlying cause has been addressed.", e );
                }
            }
        /*
         * Finally, we need to check, in an idempotent fashion, if the session state needs to be persisted.
         */
            if ( sessionTracker.logIndex() < logIndex )
            {
                sessionTracker.update( replicatedTx.globalSession(), replicatedTx.localOperationId(), logIndex );
                sessionUpdated = true;
            }
            else
            {
                log.info( format( "Rejecting log index %d since the session tracker is already at log index %d",
                        logIndex, sessionTracker.logIndex() ) );
            }
        }
        finally
        {
            if ( !( committed && sessionUpdated ) )
            {
                log.info( "Something did not go as expected. Committed is %b, sessionUpdated is %b, at log index %d",
                        committed, sessionUpdated, logIndex );
            }
        }
    }

    private boolean operationValid( ReplicatedTransaction<MEMBER> replicatedTx )
    {
        return sessionTracker.validateOperation( replicatedTx.globalSession(), replicatedTx.localOperationId() );
    }

    public void setLastCommittedIndex( long lastCommittedIndex )
    {
        this.lastCommittedIndex = lastCommittedIndex;
    }
}
