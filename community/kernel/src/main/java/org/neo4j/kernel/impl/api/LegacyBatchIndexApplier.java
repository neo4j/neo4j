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

import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.util.IdOrderingQueue;

public class LegacyBatchIndexApplier implements BatchTransactionApplier
{
    private final IdOrderingQueue transactionOrdering;
    private final TransactionApplicationMode mode;
    private final IndexConfigStore indexConfigStore;
    private final LegacyIndexApplierLookup applierLookup;
    private long activeTransactionId = -1;
    private boolean isLastTransactionInBatch = false;

    public LegacyBatchIndexApplier( IndexConfigStore indexConfigStore, LegacyIndexApplierLookup applierLookup,
            IdOrderingQueue transactionOrdering, TransactionApplicationMode mode )
    {
        this.indexConfigStore = indexConfigStore;
        this.applierLookup = applierLookup;
        this.transactionOrdering = transactionOrdering;
        this.mode = mode;
    }

    @Override
    public TransactionApplier startTx( TransactionToApply transaction ) throws IOException
    {
        if ( !transaction.commitment().hasLegacyIndexChanges() )
        {
            return TransactionApplier.EMPTY;
        }

        activeTransactionId = transaction.transactionId();
        try
        {
            transactionOrdering.waitFor( activeTransactionId );
            // Need to know if this is the last transaction in this batch of legacy index changes in order to
            // run apply before other batches are allowed to run, in order to preserve ordering.
            if ( transaction.next() == null )
            {
                isLastTransactionInBatch = true;
            }

            return new LegacyIndexTransactionApplier( isLastTransactionInBatch, applierLookup, indexConfigStore, mode,
                    transactionOrdering, activeTransactionId );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new IOException( "Interrupted while waiting for applying tx:" + activeTransactionId +
                    " legacy index updates", e );
        }
    }

    @Override
    public TransactionApplier startTx( TransactionToApply transaction, LockGroup lockGroup ) throws IOException
    {
        return startTx( transaction );
    }

    @Override
    public void close()
    {
        if ( isLastTransactionInBatch )
        {
            // Let other batches run
            notifyLegacyIndexOperationQueue();
        }
    }

    private void notifyLegacyIndexOperationQueue()
    {
        if ( activeTransactionId != -1 )
        {
            transactionOrdering.removeChecked( activeTransactionId );
            activeTransactionId = -1;
        }
    }
}
