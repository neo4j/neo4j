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

import org.neo4j.coreedge.raft.replication.session.LocalSessionPool;
import org.neo4j.coreedge.raft.replication.session.OperationContext;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.replication.Replicator.ReplicationFailedException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionFactory.createImmutableReplicatedTransaction;

public class ReplicatedTransactionCommitProcess extends LifecycleAdapter implements TransactionCommitProcess
{
    private final Replicator replicator;
    private ReplicatedTransactionStateMachine replicatedTxListener;
    private final LocalSessionPool sessionPool;

    public ReplicatedTransactionCommitProcess( Replicator replicator,
                                               LocalSessionPool sessionPool,
                                               ReplicatedTransactionStateMachine replicatedTxListener )
    {
        this.sessionPool = sessionPool;
        this.replicatedTxListener = replicatedTxListener;
        this.replicator = replicator;
        replicator.subscribe( this.replicatedTxListener );
    }

    @Override
    public long commit( final TransactionRepresentation tx, final LockGroup locks,
                        final CommitEvent commitEvent,
                        TransactionApplicationMode mode ) throws TransactionFailureException
    {
        OperationContext operationContext = sessionPool.acquireSession();

        ReplicatedTransaction transaction;
        try
        {
            transaction = createImmutableReplicatedTransaction( tx, operationContext.globalSession(), operationContext.localOperationId() );
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
                replicator.replicate( transaction );

                Long txId = futureTxId.get( 60, TimeUnit.SECONDS );
                sessionPool.releaseSession( operationContext );

                return txId;
            }
            catch ( InterruptedException | TimeoutException  e )
            {
                futureTxId.cancel( false );
            }
            catch ( ReplicationFailedException | ExecutionException e )
            {
                throw new TransactionFailureException( "Failed to commit transaction", e );
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
