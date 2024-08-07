/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.recordstorage;

import java.io.IOException;
import org.neo4j.storageengine.api.StorageEngineTransaction;

/**
 * Responsible for dealing with batches of transactions. See also {@link TransactionApplier}
 *
 * Typical usage looks like:
 * <pre>
 * TransactionApplierFactory applierFactory = getApplierFactory();
 *
 * TransactionToApply tx = batch;
 * while ( tx != null )
 * {
 *     try ( var batchContext = createBatchContext() )
 *     {
 *         try ( TransactionApplier txApplier = applierFactory.startTx( tx, batchContext ) )
 *         {
 *             tx.transactionRepresentation().accept( txApplier );
 *         }
 *     }
 *     catch ( Throwable cause )
 *     {
 *         databaseHealth.panic( cause );
 *         throw cause;
 *     }
 *     tx = tx.next();
 * }
 *
 * </pre>
 */
@FunctionalInterface
public interface TransactionApplierFactory {
    /**
     * Get the suitable {@link TransactionApplier} for a given transaction, and the store which this {@link
     * TransactionApplierFactory} is associated with.
     *
     * Typically you'd want to use this in a try-with-resources block to automatically close the {@link
     * TransactionApplier} when finished with the transaction, f.ex. as:
     * <pre>
     * try ( TransactionApplier txApplier = applierFactory.startTx( txToApply )
     * {
     *     // Apply the transaction
     *     txToApply.transactionRepresentation().accept( txApplier );
     *     // Or apply other commands
     *     // txApplier.visit( command );
     * }
     * </pre>
     *
     * @param transaction The transaction which this applier is going to apply. Once we don't have to validate index
     * updates anymore, we can change this to simply be the transactionId
     * @param batchContext context of batch apply
     * @return a {@link TransactionApplier} which can apply this transaction and other commands to the store.
     * @throws IOException on error.
     */
    TransactionApplier startTx(StorageEngineTransaction transaction, BatchContext batchContext) throws IOException;
}
