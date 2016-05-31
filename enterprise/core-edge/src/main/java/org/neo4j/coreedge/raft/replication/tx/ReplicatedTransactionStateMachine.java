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
import java.util.function.Consumer;

import org.neo4j.coreedge.raft.state.Result;
import org.neo4j.coreedge.raft.state.StateMachine;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenStateMachine;
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
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LockSessionExpired;

public class ReplicatedTransactionStateMachine<MEMBER> implements StateMachine<ReplicatedTransaction>
{
    private final ReplicatedLockTokenStateMachine<MEMBER> lockTokenStateMachine;
    private TransactionCommitProcess commitProcess;
    private final Log log;

    private long lastCommittedIndex = -1;

    public ReplicatedTransactionStateMachine( ReplicatedLockTokenStateMachine<MEMBER> lockStateMachine,
                                              LogProvider logProvider )
    {
        this.lockTokenStateMachine = lockStateMachine;
        this.log = logProvider.getLog( getClass() );
    }

    public synchronized void installCommitProcess( TransactionCommitProcess commitProcess, long lastCommittedIndex )
    {
        this.commitProcess = commitProcess;
        this.lastCommittedIndex = lastCommittedIndex;
        log.info( format("Updated lastCommittedIndex to %d", lastCommittedIndex) );
    }

    @Override
    public synchronized void flush() throws IOException
    {
        // implicity flushed
    }

    @Override
    public synchronized void applyCommand( ReplicatedTransaction replicatedTx, long commandIndex, Consumer<Result> callback )
    {
        if ( commandIndex <= lastCommittedIndex )
        {
            log.debug( "Ignoring transaction at log index %d since already committed up to %d", commandIndex, lastCommittedIndex );
            return;
        }

        TransactionRepresentation tx;

        byte[] extraHeader = encodeLogIndexAsTxHeader( commandIndex );
        tx = ReplicatedTransactionFactory.extractTransactionRepresentation( replicatedTx, extraHeader );

        int currentTokenId = lockTokenStateMachine.currentToken().id();
        int txLockSessionId = tx.getLockSessionId();

        if ( currentTokenId != txLockSessionId && txLockSessionId != Locks.Client.NO_LOCK_SESSION_ID )
        {
            callback.accept( Result.of( new TransactionFailureException( LockSessionExpired,
                    "The lock session in the cluster has changed: [current lock session id:%d, tx lock session id:%d]",
                    currentTokenId, txLockSessionId ) ) );
        }
        else
        {
            try
            {
                long txId = commitProcess.commit( new TransactionToApply( tx ), CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );
                callback.accept( Result.of( txId ) );
            }
            catch ( TransactionFailureException e )
            {
                throw new IllegalStateException( "Failed to locally commit a transaction that has already been " +
                        "committed to the RAFT log. This server cannot process later transactions and needs to be " +
                        "restarted once the underlying cause has been addressed.", e );
            }
        }
    }
}
