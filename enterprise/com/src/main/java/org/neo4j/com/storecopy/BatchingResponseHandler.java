/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.com.storecopy;

import java.io.IOException;

import org.neo4j.com.ComException;
import org.neo4j.com.Response;
import org.neo4j.com.Response.Handler;
import org.neo4j.com.storecopy.ResponseUnpacker.TxHandler;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
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
    private final VersionContextSupplier versionContextSupplier;
    private final TransactionObligationFulfiller obligationFulfiller;
    private final Log log;

    BatchingResponseHandler( int maxBatchSize, TransactionQueue.Applier applier,
            TransactionObligationFulfiller obligationFulfiller, TxHandler txHandler,
            VersionContextSupplier versionContextSupplier, Log log )
    {
        this.obligationFulfiller = obligationFulfiller;
        this.txHandler = txHandler;
        this.versionContextSupplier = versionContextSupplier;
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
                transaction.getCommitEntry().getTxId(),
                versionContextSupplier.getVersionContext() )
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
