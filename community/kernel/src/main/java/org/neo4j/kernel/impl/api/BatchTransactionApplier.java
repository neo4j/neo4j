/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api;

import java.io.IOException;

import org.neo4j.kernel.impl.locking.LockGroup;

/**
 * Responsible for dealing with batches of transactions. See also {@link TransactionApplier}
 *
 * Typical usage looks like:
 * <pre>
 * {@code
 * try ( BatchTransactionApplier batchApplier = getBatchApplier() )
 * {
 *     TransactionToApply tx = batch;
 *     while ( tx != null )
 *     {
 *         try ( LockGroup locks = new LockGroup() )
 *         {
 *             ensureValidatedIndexUpdates( tx );
 *             try ( TransactionApplier txApplier = batchApplier.startTx( tx, locks ) )
 *             {
 *                 tx.transactionRepresentation().accept( txApplier );
 *             }
 *         }
 *         catch ( Throwable cause )
 *         {
 *             databaseHealth.panic( cause );
 *             throw cause;
 *         }
 *         tx = tx.next();
 *     }
 * }
 * }
 * </pre>
 */
public interface BatchTransactionApplier extends AutoCloseable
{
    /**
     * Get the suitable {@link TransactionApplier} for a given transaction, and the store which this {@link
     * BatchTransactionApplier} is associated with. See also {@link #startTx(TransactionToApply, LockGroup)} if
     * your operations need to share a {@link LockGroup}.
     *
     * Typically you'd want to use this in a try-with-resources block to automatically close the {@link
     * TransactionApplier} when finished with the transaction, f.ex. as:
     * <pre>
     * {@code
     *     try ( TransactionApplier txApplier = batchTxApplier.startTx( txToApply )
     *     {
     *         // Apply the transaction
     *         txToApply.transactionRepresentation().accept( txApplier );
     *         // Or apply other commands
     *         // txApplier.visit( command );
     *     }
     * }
     * </pre>
     *
     * @param transaction The transaction which this applier is going to apply. Once we don't have to validate index
     * updates anymore, we can change this to simply be the transactionId
     * @return a {@link TransactionApplier} which can apply this transaction and other commands to the store.
     */
    TransactionApplier startTx( TransactionToApply transaction ) throws IOException;

    /**
     * Get the suitable {@link TransactionApplier} for a given transaction, and the store which this {@link
     * BatchTransactionApplier} is associated with. See also {@link #startTx(TransactionToApply)} if your transaction
     * does not require any locks.
     *
     * Typically you'd want to use this in a try-with-resources block to automatically close the {@link
     * TransactionApplier} when finished with the transaction, f.ex. as:
     * <pre>
     * {@code
     *     try ( TransactionApplier txApplier = batchTxApplier.startTx( txToApply )
     *     {
     *         // Apply the transaction
     *         txToApply.transactionRepresentation().accept( txApplier );
     *         // Or apply other commands
     *         // txApplier.visit( command );
     *     }
     * }
     * </pre>
     *
     * @param transaction The transaction which this applier is going to apply. Once we don't have to validate index
     * updates anymore, we can change this to simply be the transactionId
     * @param lockGroup A lockGroup which can hold the locks that the transaction requires.
     * @return a {@link TransactionApplier} which can apply this transaction and other commands to the store.
     */
    TransactionApplier startTx( TransactionToApply transaction, LockGroup lockGroup ) throws IOException;

    /**
     * This method is suitable for any work that needs to be done after a batch of transactions. Typically called
     * implicitly at the end of a try-with-resources block.
     *
     * @throws Exception
     */
    @Override
    void close() throws Exception;

}
