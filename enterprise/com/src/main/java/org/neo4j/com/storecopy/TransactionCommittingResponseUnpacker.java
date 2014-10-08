/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.com.storecopy.TransactionQueue.TransactionVisitor;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Receives and unpacks {@link Response responses}.
 * Transaction obligations are handled by {@link TransactionObligationFulfiller} and
 * {@link TransactionStream transaction streams} are {@link TransactionRepresentationStoreApplier applied to the store},
 * in batches.
 *
 * It is assumed that any {@link TransactionStreamResponse response carrying transaction data} comes from the one
 * and same thread.
 */
public class TransactionCommittingResponseUnpacker implements ResponseUnpacker, Lifecycle
{
    private static final int DEFAULT_BATCH_SIZE = 30;

    private class BatchingResponseHandler implements Response.Handler,
            Visitor<CommittedTransactionRepresentation, IOException>
    {
        private final TxHandler txHandler;

        private BatchingResponseHandler( TxHandler txHandler )
        {
            this.txHandler = txHandler;
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
            catch ( InterruptedException e )
            {
                throw new IOException( e );
            }
        }

        @Override
        public Visitor<CommittedTransactionRepresentation, IOException> transactions()
        {
            return this;
        }

        @Override
        public boolean visit( CommittedTransactionRepresentation transaction ) throws IOException
        {
            if ( transactionQueue.queue( transaction, txHandler ) )
            {
                applyQueuedTransactions();
            }
            return true;
        }
    }

    private final DependencyResolver resolver;

    private TransactionAppender appender;
    private TransactionRepresentationStoreApplier storeApplier;
    private TransactionIdStore transactionIdStore;
    private TransactionObligationFulfiller obligationFulfiller;
    private final TransactionQueue transactionQueue;
    private volatile boolean stopped = false;

    // Visits all queued transactions, appending them to the log
    private final TransactionVisitor batchAppender = new TransactionVisitor()
    {
        @Override
        public void visit( CommittedTransactionRepresentation transaction, TxHandler handler ) throws IOException
        {
            appender.append( transaction.getTransactionRepresentation(), transaction.getCommitEntry().getTxId() );
        }
    };

    // Visits all queued, and recently appended, transactions, applying them to the store
    private final TransactionVisitor batchApplier = new TransactionVisitor()
    {
        @Override
        public void visit( CommittedTransactionRepresentation transaction, TxHandler handler ) throws IOException
        {
            long transactionId = transaction.getCommitEntry().getTxId();
            transactionIdStore.transactionCommitted( transactionId );
            try
            {
                try ( LockGroup locks = new LockGroup() )
                {
                    storeApplier.apply( transaction.getTransactionRepresentation(), locks,
                            transactionId, TransactionApplicationMode.EXTERNAL );
                    handler.accept( transaction );
                }
            }
            finally
            {
                transactionIdStore.transactionClosed( transactionId );
            }
        }
    };

    public TransactionCommittingResponseUnpacker( DependencyResolver resolver )
    {
        this( resolver, DEFAULT_BATCH_SIZE );
    }

    public TransactionCommittingResponseUnpacker( DependencyResolver resolver, int maxBatchSize )
    {
        this.resolver = resolver;
        this.transactionQueue = new TransactionQueue( maxBatchSize );
    }

    @Override
    public void unpackResponse( Response<?> response, final TxHandler txHandler ) throws IOException
    {
        if ( stopped )
        {
            throw new IllegalStateException( "Component is currently stopped" );
        }

        response.accept( new BatchingResponseHandler( txHandler ) );

        if ( response.hasTransactionsToBeApplied() )
        {
            applyQueuedTransactions();
        }
    }

    private void applyQueuedTransactions() throws IOException
    {
        if ( transactionQueue.acceptAndKeep( batchAppender ) > 0 )
        {
            // TODO if this instance is set to "slave_only" then we can actually skip the force call here.
            // Reason being that even if there would be a reordering in some layer where a store file would be
            // changed before that change would have ended up in the log, it would be fine sine as a slave
            // you would pull that transaction again anyhow before making changes to (after reading) any record.
            appender.force();
            transactionQueue.acceptAndRemove( batchApplier );
        }
    }

    @Override
    public void init() throws Throwable
    {   // Nothing to init
    }

    @Override
    public void start() throws Throwable
    {
        this.appender = resolver.resolveDependency( LogicalTransactionStore.class ).getAppender();
        this.storeApplier = resolver.resolveDependency( TransactionRepresentationStoreApplier.class );
        this.transactionIdStore = resolver.resolveDependency( TransactionIdStore.class );
        this.obligationFulfiller = resolveTransactionObligationFulfiller( resolver );
        this.stopped = false;
    }

    private static TransactionObligationFulfiller resolveTransactionObligationFulfiller( DependencyResolver resolver )
    {
        try
        {
            return resolver.resolveDependency( TransactionObligationFulfiller.class );
        }
        catch ( IllegalArgumentException e )
        {
            return new TransactionObligationFulfiller()
            {
                @Override
                public void fulfill( long toTxId )
                {
                    throw new UnsupportedOperationException( "Should not be called" );
                }
            };
        }
    }

    @Override
    public void stop() throws Throwable
    {
        this.stopped = true;
    }

    @Override
    public void shutdown() throws Throwable
    {   // Nothing to shut down
    }
}
