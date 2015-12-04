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
import java.util.ArrayList;

import org.neo4j.kernel.impl.locking.LockGroup;

/**
 * Responsible for dealing with batches of transactions. See also {@link TransactionApplier}
 *
 * Typical usage looks like:
 * <pre>
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
 * </pre>
 */
public interface BatchTransactionApplier extends AutoCloseable
{
    // Once we don't have to validate index updates anymore, we can change this to simply be the transactionId
    TransactionApplier startTx( TransactionToApply transaction ) throws IOException;
    // If transactions need to share lock group
    TransactionApplier startTx( TransactionToApply transaction, LockGroup lockGroup ) throws IOException;

    class BatchTransactionApplierFacade implements BatchTransactionApplier
    {

        private final BatchTransactionApplier[] appliers;

        public BatchTransactionApplierFacade( BatchTransactionApplier... appliers )
        {
            this.appliers = appliers;
        }

        @Override
        public TransactionApplier startTx( TransactionToApply transaction ) throws IOException
        {
            ArrayList<TransactionApplier> txAppliers = new ArrayList<>();
            for ( BatchTransactionApplier applier : appliers )
            {
                txAppliers.add( applier.startTx( transaction ) );
            }
            return new TransactionApplier.TransactionApplierFacade(
                    txAppliers.toArray( new TransactionApplier[appliers.length] ) );
        }

        @Override
        public TransactionApplier startTx( TransactionToApply transaction, LockGroup lockGroup ) throws IOException
        {
            ArrayList<TransactionApplier> txAppliers = new ArrayList<>();
            for ( BatchTransactionApplier applier : appliers )
            {
                txAppliers.add( applier.startTx( transaction, lockGroup ) );
            }
            return new TransactionApplier.TransactionApplierFacade(
                    txAppliers.toArray( new TransactionApplier[appliers.length] ) );
        }

        @Override
        public void close() throws Exception
        {
            // Not sure why it is necessary to close them in reverse order
            for ( int i = appliers.length; i-- > 0; )
            {
                appliers[i].close();
            }
        }
    }
}
