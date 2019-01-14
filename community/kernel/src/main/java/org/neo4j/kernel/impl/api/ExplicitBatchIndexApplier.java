/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.api;

import java.io.IOException;

import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.TransactionApplicationMode;

/**
 * This class reuses the same {@link ExplicitIndexTransactionApplier} for all transactions in the batch for performance
 * reasons. The {@link ExplicitIndexTransactionApplier} contains appliers specific for each {@link IndexCommand} which
 * are closed here on the batch level in {@link #close()}, before the last transaction locks are released.
 */
public class ExplicitBatchIndexApplier extends BatchTransactionApplier.Adapter
{
    private final IdOrderingQueue transactionOrdering;
    private final TransactionApplicationMode mode;
    private final IndexConfigStore indexConfigStore;
    private final ExplicitIndexApplierLookup applierLookup;

    // There are some expensive lookups made in the TransactionApplier, so cache it
    private ExplicitIndexTransactionApplier txApplier;
    private long lastTransactionId = -1;

    public ExplicitBatchIndexApplier( IndexConfigStore indexConfigStore, ExplicitIndexApplierLookup applierLookup,
            IdOrderingQueue transactionOrdering, TransactionApplicationMode mode )
    {
        this.indexConfigStore = indexConfigStore;
        this.applierLookup = applierLookup;
        this.transactionOrdering = transactionOrdering;
        this.mode = mode;
    }

    @Override
    public TransactionApplier startTx( CommandsToApply transaction ) throws IOException
    {
        long activeTransactionId = transaction.transactionId();
        try
        {
            // Cache transactionApplier because it has some expensive lookups
            if ( txApplier == null )
            {
                txApplier = new ExplicitIndexTransactionApplier( applierLookup, indexConfigStore, mode,
                        transactionOrdering );
            }

            if ( transaction.requiresApplicationOrdering() )
            {
                // Index operations must preserve order so wait for previous tx to finish
                transactionOrdering.waitFor( activeTransactionId );
                // And set current tx so we can notify the next transaction when we are finished
                if ( transaction.next() != null )
                {
                    // Let each transaction notify the next
                    txApplier.setTransactionId( activeTransactionId );
                }
                else
                {
                    // except the last transaction, which notifies that it is done after appliers have been closed
                    lastTransactionId = activeTransactionId;
                }
            }

            return txApplier;
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new IOException( "Interrupted while waiting for applying tx:" + activeTransactionId +
                    " explicit index updates", e );
        }
    }

    @Override
    public void close() throws Exception
    {
        if ( txApplier == null )
        {
            // Never started a transaction, so nothing to do
            return;
        }

        for ( TransactionApplier applier : txApplier.applierByProvider.values() )
        {
            applier.close();
        }

        // Allow other batches to run
        if ( lastTransactionId != -1 )
        {
            transactionOrdering.removeChecked( lastTransactionId );
            lastTransactionId = -1;
        }
    }
}
