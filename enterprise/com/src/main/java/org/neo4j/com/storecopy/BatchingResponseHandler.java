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
package org.neo4j.com.storecopy;

import java.io.IOException;

import org.neo4j.com.ComException;
import org.neo4j.com.Response;
import org.neo4j.com.Response.Handler;
import org.neo4j.com.storecopy.ResponseUnpacker.TxHandler;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
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

    private final KernelTransactions kernelTransactions;
    private final long idReuseSafeZoneTime;

    public BatchingResponseHandler( int maxBatchSize, TransactionQueue.Applier applier,
            TransactionObligationFulfiller obligationFulfiller, TxHandler txHandler, Log log,
            KernelTransactions kernelTransactions, long idReuseSafeZoneTime )
    {
        this.obligationFulfiller = obligationFulfiller;
        this.txHandler = txHandler;
        this.queue = new TransactionQueue( maxBatchSize, applier );
        this.log = log;
        this.kernelTransactions = kernelTransactions;
        this.idReuseSafeZoneTime = idReuseSafeZoneTime;
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
        boolean batchSizeReached = this.queue.queue( new TransactionToApply(
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

        if ( batchSizeReached )
        {
            applyQueuedTransactionsIfNeeded();
        }

        return false;
    }

    public void applyQueuedTransactionsIfNeeded() throws Exception
    {
        if ( queue.isEmpty() )
        {
            return;
        }

        /*
          Case 1 (Not really a problem):
           - chunk of batch is smaller than safe zone
           - tx started after activeTransactions() is called
           is safe because those transactions will see the latest state of store before chunk is applied and
           because chunk is smaller than safe zone we are guarantied to not see two different states of any record
           when applying the chunk.

             activeTransactions() is called
             |        start committing chunk
          ---|----+---|--|------> TIME
                  |      |
                  |      Start applying chunk
                  New tx starts here. Does not get terminated because not among active transactions, this is safe.

          Case 2:
           - chunk of batch is larger than safe zone
           - tx started after activeTransactions() but before apply

             activeTransactions() is called
             |        start committing chunk
          ---|--------|+-|------> TIME
                       | |
                       | Start applying chunk
                       New tx starts here. Does not get terminated because not among active transactions, but will
                       read outdated data and can be affected by reuse contamination.
         */

        if ( batchSizeExceedsSafeZone() )
        {
            // We stop new transactions from starting to avoid problem described in (2)
            kernelTransactions.blockNewTransactions();
            try
            {
                markUnsafeTransactionsForTermination();
                queue.empty();
            }
            finally
            {
                kernelTransactions.unblockNewTransactions();
            }
        }
        else
        {
            markUnsafeTransactionsForTermination();
            queue.empty();
        }
    }

    private boolean batchSizeExceedsSafeZone()
    {
        long lastAppliedTimestamp = queue.last().transactionRepresentation().getTimeCommitted();
        long firstAppliedTimestamp = queue.first().transactionRepresentation().getTimeCommitted();
        long chunkLength = lastAppliedTimestamp - firstAppliedTimestamp;

        return chunkLength > idReuseSafeZoneTime;
    }

    private void markUnsafeTransactionsForTermination()
    {
        long lastAppliedTimestamp = queue.last().transactionRepresentation().getTimeCommitted();
        long earliestSafeTimestamp = lastAppliedTimestamp - idReuseSafeZoneTime;

        for ( KernelTransaction tx : kernelTransactions.activeTransactions() )
        {
            long commitTimestamp = tx.lastTransactionTimestampWhenStarted();

            if ( commitTimestamp != TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP &&
                 commitTimestamp < earliestSafeTimestamp )
            {
                tx.markForTermination( Status.Transaction.Outdated );
            }
        }
    }
}
