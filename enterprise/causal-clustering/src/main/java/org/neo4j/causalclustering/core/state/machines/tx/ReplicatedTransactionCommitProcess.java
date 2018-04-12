/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.machines.tx;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionFactory.createImmutableReplicatedTransaction;

public class ReplicatedTransactionCommitProcess implements TransactionCommitProcess
{
    private final Replicator replicator;

    public ReplicatedTransactionCommitProcess( Replicator replicator )
    {
        this.replicator = replicator;
    }

    @Override
    public long commit( final TransactionToApply tx,
                        final CommitEvent commitEvent,
                        TransactionApplicationMode mode ) throws TransactionFailureException
    {
        ReplicatedTransaction transaction = createImmutableReplicatedTransaction( tx.transactionRepresentation() );
        Future<Object> futureTxId;
        try
        {
            futureTxId = replicator.replicate( transaction, true );
        }
        catch ( InterruptedException e )
        {
            throw new TransactionFailureException( "Interrupted replicating transaction.", e );
        }

        try
        {
            return (long) futureTxId.get();
        }
        catch ( ExecutionException e )
        {
            if ( e.getCause() instanceof TransactionFailureException )
            {
                throw (TransactionFailureException) e.getCause();
            }
            // TODO: Panic?
            throw new RuntimeException( e );
        }
        catch ( InterruptedException e )
        {
            // TODO Wait for the transaction to possibly finish within a user configurable time, before aborting.
            throw new TransactionFailureException( "Interrupted while waiting for txId", e );
        }
    }
}
