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
package org.neo4j.com.storecopy;

import java.io.IOException;

import org.neo4j.com.ComException;
import org.neo4j.com.Response;
import org.neo4j.com.Response.Handler;
import org.neo4j.com.storecopy.ResponseUnpacker.TxHandler;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.api.TransactionQueue;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.Commitment;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.Log;

/**
 * {@link Handler Response handler} which commits received transactions (for transaction stream responses)
 * in batches. Can fulfill transaction obligations.
 */
class BatchingResponseHandler implements Response.Handler,
        Visitor<CommittedTransactionRepresentation,Exception>
{
    private final TransactionQueue queue;
    private final TxHandler txHandler;
    private final TransactionObligationFulfiller obligationFulfiller;
    private final Log log;

    public BatchingResponseHandler( int maxBatchSize, TransactionQueue.Applier applier,
            TransactionObligationFulfiller obligationFulfiller, TxHandler txHandler, Log log )
    {
        this.obligationFulfiller = obligationFulfiller;
        this.txHandler = txHandler;
        this.queue = new TransactionQueue( maxBatchSize, applier );
        this.log = log;
    }

    @Override
    public void obligation( long txId ) throws IOException
    {
        if ( txId == TransactionIdStore.BASE_TX_ID )
        {   // Means "empty" response
            return;
        }

        try
        {
            obligationFulfiller.fulfill( txId );
        }
        catch ( IllegalStateException e )
        {
            throw new ComException( "Failed to pull updates", e )
                    .traceComException( log, "BatchingResponseHandler.obligation" );
        }
        catch ( InterruptedException e )
        {
            throw new IOException( e );
        }
    }

    @Override
    public Visitor<CommittedTransactionRepresentation,Exception> transactions()
    {
        return this;
    }

    @Override
    public boolean visit( CommittedTransactionRepresentation transaction ) throws Exception
    {
        queue.queue( new TransactionToApply(
                transaction.getTransactionRepresentation(),
                transaction.getCommitEntry().getTxId() )
        {
            @Override
            public void commitment( Commitment commitment, long transactionId )
            {
                // TODO Perhaps odd to override this method here just to be able to call txHandler?
                super.commitment( commitment, transactionId );
                txHandler.accept( transactionId );
            }
        } );
        return false;
    }

    void applyQueuedTransactions() throws Exception
    {
        queue.empty();
    }
}
