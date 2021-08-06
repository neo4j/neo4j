/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.database;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.LinkedTransactionError;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionCommitFailed;

/**
 * This Handler provides the ability to link transactions using {@link #registerInnerTransaction(long, long)}
 * and to unlink them again using {@link #removeInnerTransaction(long, long)}.
 * <p>
 * For linked transactions the guarantees are:
 * <ul>
 *     <li>The inner transaction will be terminated if the outer transaction gets terminated.</li>
 *      <li>The inner transaction will be terminated if the outer transaction gets rolled back.</li>
 *      <li>The outer transaction cannot commit if it is linked to inner transactions.</li>
 * </ul>
 */
public class DatabaseLinkedTransactionsHandler extends TransactionEventListenerAdapter<Object>
{

    private final KernelTransactions transactions;
    /**
     * Maps a transaction's user id to its inner transactions' user ids.
     */
    Map<Long,Set<Long>> innerTransactions = new ConcurrentHashMap<>();

    public DatabaseLinkedTransactionsHandler( KernelTransactions kernelTransactions )
    {
        this.transactions = kernelTransactions;
    }

    @Override
    public void beforeTerminate( long outerTransactionId, Status reason )
    {
        Set<Long> innerTransactionIds = innerTransactions.put( outerTransactionId, Collections.emptySet() );
        Set<KernelTransactionHandle> handles = transactions.executingTransactions();
        if ( innerTransactionIds != null )
        {
            innerTransactionIds.forEach( innerTransactionId -> terminateInnerTransaction( reason, handles, innerTransactionId ) );
        }
    }

    @Override
    public void beforeCloseTransaction( long transactionId, boolean canCommit ) throws Exception
    {
        if ( canCommit && hasInnerTransaction( transactionId ) )
        {
            throw new TransactionFailureException( TransactionCommitFailed, "The transaction cannot be committed when it has open inner transactions." );
        }
        else
        {
            Set<Long> innerTransactionIds = innerTransactions.get( transactionId );
            Set<KernelTransactionHandle> handles = transactions.executingTransactions();
            if ( innerTransactionIds != null )
            {
                innerTransactionIds.forEach( innerTransactionId -> terminateInnerTransaction( LinkedTransactionError, handles, innerTransactionId ) );
            }
        }
    }

    private void terminateInnerTransaction( Status reason, Set<KernelTransactionHandle> handles, Long innerTransactionId )
    {
        getTransactionHandlesById( handles, innerTransactionId )
                .forEach( handle -> handle.markForTermination( reason ) );
    }

    private Stream<KernelTransactionHandle> getTransactionHandlesById( Set<KernelTransactionHandle> handles, long id )
    {
        return handles.stream()
                      .filter( handle -> handle.getUserTransactionId() == id );
    }

    /**
     * Link an inner and an outer transaction.
     * The inner transaction will be terminated if the outer transaction gets terminated.
     * The inner transaction will be terminated if the outer transaction gets rolled back.
     * The outer transaction cannot commit if it is linked to inner transactions.
     */
    public void registerInnerTransaction( long innerTransactionId, long outerTransactionId )
    {
        Set<Long> innerTransactionIds = innerTransactions.computeIfAbsent( outerTransactionId, ignore -> ConcurrentHashMap.newKeySet() );
        try
        {
            innerTransactionIds.add( innerTransactionId );
        }
        catch ( UnsupportedOperationException e )
        {
            // if the set is immutable that means that termination has begun
            Set<KernelTransactionHandle> handles = transactions.executingTransactions();
            terminateInnerTransaction( Terminated, handles, innerTransactionId );
        }
    }

    /**
     * Remove a link between an inner and an outer transaction
     */
    public void removeInnerTransaction( long innerTransactionId, long outerTransactionId )
    {
        Set<Long> innerTransactionIds = innerTransactions.computeIfAbsent( outerTransactionId, ignore -> ConcurrentHashMap.newKeySet() );
        try
        {
            innerTransactionIds.remove( innerTransactionId );
        }
        catch ( UnsupportedOperationException e )
        {
            // If the set is immutable that means that termination has begun, but we cannot do anything anymore because inner transaction has already been
            // committed.
        }
    }

    /**
     * @return {@code true} if the transaction with the given id is linked with an inner transaction, {@code false} otherwise.
     */
    public boolean hasInnerTransaction( long outerTransactionId )
    {
        return innerTransactions.containsKey( outerTransactionId ) && !innerTransactions.get( outerTransactionId ).isEmpty();
    }
}
